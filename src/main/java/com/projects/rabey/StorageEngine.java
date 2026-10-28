package com.projects.rabey;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class StorageEngine {

    private final Object rotationLock = new Object();
    private final WALWriter walWriter;
    private final SSTTableReaderManager sstTableReaderManager;
    private boolean walEnabled = true;
    private volatile MemTable activeMemTable = new MemTable();
    private final ConcurrentLinkedDeque<MemTable> immutableMemTables = new ConcurrentLinkedDeque<>();
    private static final long DEFAULT_FLUSH_THRESHOLD_BYTES = 64L * 1024L * 1024L; // 64MB
    private final long flushThresholdBytes;
    private static final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final SSTTableWriter SSTableWriter;
    private final AtomicBoolean flushQueued = new AtomicBoolean(false);
    private final AtomicReference<Throwable> flushFailure = new AtomicReference<>();

    // default constructor keeps previous behavior
    public StorageEngine(WALWriter walWriter, SSTTableWriter SSTableWriter,
                    SSTTableReaderManager sstTableReaderManager) {
        this(walWriter, SSTableWriter, sstTableReaderManager, DEFAULT_FLUSH_THRESHOLD_BYTES);
    }

    // new constructor to allow tests to set a lower flush threshold
    public StorageEngine(WALWriter walWriter, SSTTableWriter SSTableWriter, SSTTableReaderManager sstTableReaderManager,
                    long flushThresholdBytes) {
        this.walWriter = walWriter;
        this.SSTableWriter = SSTableWriter;
        this.sstTableReaderManager = sstTableReaderManager;
        this.flushThresholdBytes = flushThresholdBytes;
    }

    public void put(byte[] key, byte[] value) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        throwIfFlushFailed();
        try {
            if (walEnabled) {
                this.walWriter.write(key, value);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to write to WAL", e);
        }

        long currentSize = activeMemTable.put(key, value);
        if (currentSize >= flushThresholdBytes) {
            rotateMemTableIfNeeded();
            flushImmutableMemTables();
        }
    }

    private void rotateMemTableIfNeeded() {
        if (activeMemTable.getByteSize() >= flushThresholdBytes) {
            synchronized (rotationLock) {
                if (activeMemTable.getByteSize() >= flushThresholdBytes) {
                    MemTable oldTable = activeMemTable;
                    // Publish the old table before swapping so readers never miss it.
                    immutableMemTables.addFirst(oldTable);
                    activeMemTable = new MemTable();
                }
            }
        }
    }

    private void flushImmutableMemTables() {
        if (!flushQueued.compareAndSet(false, true)) {
            return;
        }
        try {
            executor.submit(() -> {
                try {
                    MemTable flushTable;
                    // Keep the table visible in the immutable queue until its SSTable is durable.
                    while ((flushTable = immutableMemTables.peekLast()) != null) {
                        SSTableWriter.writeSSTable(flushTable.getEntries());
                        immutableMemTables.removeLastOccurrence(flushTable);
                    }
                } catch (Throwable e) {
                    flushFailure.compareAndSet(null, e);
                } finally {
                    flushQueued.set(false);
                    if (flushFailure.get() == null && !immutableMemTables.isEmpty()) {
                        flushImmutableMemTables();
                    }
                }
            });
        } catch (RuntimeException e) {
            flushQueued.set(false);
            throw e;
        }
    }

    /**
     * Waits until every queued MemTable has been written to a durable SSTable and
     * rethrows any background flush failure.
     */
    public void awaitFlush() {
        try {
            while (true) {
                executor.submit(() -> {
                }).get();
                throwIfFlushFailed();
                if (immutableMemTables.isEmpty() && !flushQueued.get()) {
                    return;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for SSTable flush", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("SSTable flush task failed", e.getCause());
        }
    }

    public byte[] get(Object key) throws IOException {
        if (!(key instanceof byte[]))
            throw new IllegalArgumentException("Key must be a byte array");

        throwIfFlushFailed();
        byte[] val = activeMemTable.get((byte[]) key);
        if (val != null)
            return val;

        for (MemTable memTable : immutableMemTables) {
            val = memTable.get((byte[]) key);
            if (val != null)
                return val;
        }
        try {
            val = sstTableReaderManager.get((byte[]) key, SSTableWriter.getSSTableFileIndex());
            return val;
        } catch (IOException e) {
            throw new RuntimeException("Failed to load SSTable readers", e);
        }
    }

    private void throwIfFlushFailed() {
        Throwable failure = flushFailure.get();
        if (failure != null) {
            throw new RuntimeException("Background SSTable flush failed", failure);
        }
    }

    public void setWalEnabled(boolean b) {
        this.walEnabled = b;
    }

    public long size() {
        return this.activeMemTable.getEntryCount();
    }
}
