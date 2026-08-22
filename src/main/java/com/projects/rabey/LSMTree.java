package com.projects.rabey;

import java.util.concurrent.ConcurrentSkipListMap;

public class LSMTree extends ConcurrentSkipListMap<byte[], byte[]> {

    private final WALWriter walWriter;

    public LSMTree(WALWriter walWriter) {
        super(new ByteArrayComparator());
        this.walWriter = walWriter;
    }

    @Override
    public byte[] put(byte[] key, byte[] value) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        try {
            this.walWriter.write(key, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to write to WAL", e);
        }

        return super.put(key, value);
    }

    @Override
    public byte[] get(Object key) {
        if (!(key instanceof byte[]))
            throw new IllegalArgumentException("Key must be a byte array");

        return super.get(key);
    }
}
