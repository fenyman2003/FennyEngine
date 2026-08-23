package com.projects.rabey;

import java.io.*;
import java.nio.file.Path;

public class WALReader implements Closeable {

    private final FileInputStream fileInputStream;
    private final DataInputStream in;

    public WALReader(Path walFilePath) throws IOException {
        this.fileInputStream = new FileInputStream(walFilePath.toFile());
        BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
        this.in = new DataInputStream(bufferedInputStream);
    }

    public synchronized WALRecord readNext() throws IOException {
        int keyLength;
        try {
            // Reaching EOF cleanly on the first read indicates end-of-file
            keyLength = in.readInt();
        } catch (EOFException e) {
            return null; // Normal end of log file
        }

        try {
            int valueLength = in.readInt();

            byte[] key = new byte[keyLength];
            byte[] value = new byte[valueLength];

            in.readFully(key);
            in.readFully(value);

            return new WALRecord(key, value);
        } catch (EOFException e) {
            // Handle partial/torn writes at the end of the file gracefully
            System.err.println("Warning: Encountered truncated record at the end of WAL file.");
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        in.close();
    }
}
