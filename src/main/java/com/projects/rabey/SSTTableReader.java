package com.projects.rabey;

import java.io.*;
import java.nio.file.Path;
import java.util.Arrays;

import static com.projects.rabey.SSTTableWriter.sstableDirPath;

public class SSTTableReader implements AutoCloseable {

    private final FileInputStream fileInputStream;
    private final DataInputStream in;

    public SSTTableReader(Path sstableDirPath) throws IOException {
        this.fileInputStream = new FileInputStream(sstableDirPath.toFile());
        BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
        this.in = new DataInputStream(bufferedInputStream);
    }

    public synchronized byte[] search(byte[] searchKey, int searchKeyLength) throws IOException {
        int keyLength;
        int valueLength;
        try {
            // Reaching EOF cleanly on the first read indicates end-of-file
            keyLength = in.readInt();
            if (keyLength != searchKeyLength) {
                // Skip the value length and the key-value pair if lengths don't match
                valueLength = in.readInt(); // Read value length
                in.skipBytes(keyLength + valueLength); // Skip key and value
                return null; // Key not found
            }
        } catch (EOFException e) {
            return null; // Normal end of log file
        }

        try {
            valueLength = in.readInt();

            byte[] key = new byte[keyLength];
            in.readFully(key);
            if (!Arrays.equals(key, searchKey)) {
                // Skip the value if the key doesn't match
                in.skipBytes(valueLength);
                return null;
            }

            byte[] value = new byte[valueLength];
            in.readFully(value);

            return value;
        } catch (EOFException e) {
            // Handle partial/torn writes at the end of the file gracefully
            System.err.println("Warning: Encountered truncated record at the end of WAL file.");
            return null;
        }
    }

    public synchronized byte[] pointSearch(byte[] searchKey) throws IOException {
        try {
            int searchKeyLength = searchKey.length;
            byte[] result = null;
            while (true) {
                result = search(searchKey, searchKeyLength);
                if (result != null) {
                    break;
                }
            }
            return result; // Replace with actual implementation
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void close() throws IOException {
        in.close();
    }
}
