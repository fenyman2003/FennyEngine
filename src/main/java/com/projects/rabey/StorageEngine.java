package com.projects.rabey;

import java.util.concurrent.ConcurrentLinkedDeque;

public class StorageEngine {

    private final Object rotationLock = new Object();
    private final WALWriter walWriter;
    private boolean walEnabled = true;
    private volatile MemTable activeMemTable = new MemTable();
    private final ConcurrentLinkedDeque<MemTable> immutableMemTables = new ConcurrentLinkedDeque<>();
    public static final int FLUSH_THRESHOLD_BYTES = 64 * 1024 * 1024; // 64MB

    public StorageEngine(WALWriter walWriter) {
        this.walWriter = walWriter;
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

    public byte[] get(Object key) {
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
        return null;
    }

    public void setWalEnabled(boolean b) {
        this.walEnabled = b;
    }

    public long size() {
        return this.activeMemTable.getEntryCount();
    }
}
