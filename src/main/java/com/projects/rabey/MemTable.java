package com.projects.rabey;

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class MemTable {
    private final ConcurrentSkipListMap<byte[], byte[]> map = new ConcurrentSkipListMap<>(new ByteArrayComparator());
    private final AtomicLong byteSize = new AtomicLong(0);

    public long put(byte[] key, byte[] value) {
        map.put(key, value);
        return byteSize.addAndGet(key.length + value.length);
    }

    public byte[] get(byte[] key) {
        return map.get(key);
    }

    public long getByteSize() {
        return byteSize.get();
    }

    public long getEntryCount() {
        return map.size();
    }
}
