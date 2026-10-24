package com.projects.rabey;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;

public class SSTTableReader implements AutoCloseable {

    private static final ByteArrayComparator KEY_COMPARATOR = new ByteArrayComparator();

    private final FileInputStream fileInputStream;
    private final DataInputStream in;

    public SSTTableReader(Path sstableDirPath) throws IOException {
        this.fileInputStream = new FileInputStream(sstableDirPath.toFile());
        BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
        this.in = new DataInputStream(bufferedInputStream);
    }

    /**
     * Sequential scan over the sorted SSTable records.
     * Stops as soon as a record key strictly greater than {@code searchKey} is
     * encountered (records are ordered), or on clean EOF.
     *
     * @return the matching value, or {@code null} if the key is absent.
     */
    public byte[] search(byte[] searchKey, int searchKeyLength) throws IOException {
        int keyLength;
        int valueLength;
        int cmp;
        while (true) {
            try {
                keyLength = in.readInt();
            } catch (EOFException e) {
                return null; // Normal end of file
            }

            valueLength = in.readInt();

            byte[] key = new byte[keyLength];
            in.readFully(key);

            cmp = KEY_COMPARATOR.compare(key, searchKey);
            if (cmp == 0) {
                byte[] value = new byte[valueLength];
                in.readFully(value);
                return value;
            }
            if (cmp > 0) {
                // Records are sorted; we've passed the point where searchKey would be.
                return null;
            }
            // key < searchKey: skip the value and continue scanning.
            in.readFully(new byte[valueLength]);
        }
    }

    public byte[] pointSearch(byte[] searchKey) {
        try {
            return search(searchKey, searchKey.length);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        in.close();
    }
}
