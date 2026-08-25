package com.projects.rabey;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class StorageEngine {

    private final Object rotationLock = new Object();
    private final WALWriter walWriter;
    private final SSTTableReaderManager sstTableReaderManager;
    private boolean walEnabled = true;
    private volatile MemTable activeMemTable = new MemTable();
    private final ConcurrentLinkedDeque<MemTable> immutableMemTables = new ConcurrentLinkedDeque<>();
    public static final int FLUSH_THRESHOLD_BYTES = 64 * 1024 * 1024; // 64MB
    private static final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final SSTTableWriter SSTableWriter;

    public StorageEngine(WALWriter walWriter, SSTTableWriter SSTableWriter,
                    SSTTableReaderManager sstTableReaderManager) {
        this.walWriter = walWriter;
        this.SSTableWriter = SSTableWriter;
        this.sstTableReaderManager = sstTableReaderManager;
    }

    public void put(byte[] key, byte[] value) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        try {
            if (walEnabled) {
                this.walWriter.write(key, value);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to write to WAL", e);
        }

        long currentSize = activeMemTable.put(key, value);
        if (currentSize >= FLUSH_THRESHOLD_BYTES) {
            rotateMemTableIfNeeded();
            flushImmutableMemTables();
        }
    }

    private void rotateMemTableIfNeeded() {
        if (activeMemTable.getByteSize() >= FLUSH_THRESHOLD_BYTES) {
            synchronized (rotationLock) {
                if (activeMemTable.getByteSize() >= FLUSH_THRESHOLD_BYTES) {
                    MemTable oldTable = activeMemTable;
                    activeMemTable = new MemTable(); // Active table swapped instantly
                    immutableMemTables.addFirst(oldTable); // Immutable table safely preserved
                }
            }
        }
    }

    private void flushImmutableMemTables() {
        executor.submit(() -> {
            try {
                while (true) {
                    MemTable flushTable = immutableMemTables.pollLast();
                    if (flushTable == null) {
                        break;
                    }
                    SSTableWriter.createNewSSTTableFile();
                    for (var entry : flushTable.getEntries()) {
                        SSTableWriter.write(entry.getKey(), entry.getValue());
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to flush MemTable to SSTable", e);
            }
        });
    }

    public byte[] get(Object key) throws IOException {
        if (!(key instanceof byte[]))
            throw new IllegalArgumentException("Key must be a byte array");

        byte[] val = activeMemTable.get((byte[]) key);
        if (val != null)
            return val;

        for (MemTable memTable : immutableMemTables) {
            val = memTable.get((byte[]) key);
            if (val != null)
                return val;
        }
        try {
            val = sstTableReaderManager.get((byte[]) key);
            return val;
        } catch (IOException e) {
            throw new RuntimeException("Failed to load SSTable readers", e);
        }
    }

    public void setWalEnabled(boolean b) {
        this.walEnabled = b;
    }

    public long size() {
        return this.activeMemTable.getEntryCount();
    }
}
