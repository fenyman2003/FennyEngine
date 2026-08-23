package com.projects.rabey;

import java.io.*;
import java.nio.file.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WALWriter implements AutoCloseable {

    private final Path walDirPath;
    private static final long MAX_WAL_SIZE = 64L * 1024L; // 64KB rollover threshold
    private static final int BUFFER_SIZE = 8 * 1024; // 8KB buffer size
    private AtomicLong totalWritten;
    private DataOutputStream out;
    private FileOutputStream fileOutputStream;
    private int currentIndex;

    // Construct with directory where wal files (wal_0001, wal_0002...) are stored
    public WALWriter(Path walDirPath) throws IOException {
        this.walDirPath = walDirPath;
        if (!Files.exists(walDirPath)) {
            Files.createDirectories(walDirPath);
        }
        this.currentIndex = findNextIndex(walDirPath);
        openNewWalFile();
    }

    // Append key length | value length | key | value to the WAL file
    public synchronized void write(byte[] key, byte[] value) {
        if (key == null || value == null) {
            throw new IllegalArgumentException("Key and Value cannot be null");
        }

        int dataWriteLength = 4 + 4 + key.length + value.length;
        long expectedTotal = totalWritten.get() + dataWriteLength;
        try {
            if (expectedTotal > MAX_WAL_SIZE) {
                rotateWalFile();
            }
            createDataWrite(key, value);

            totalWritten.addAndGet(dataWriteLength);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write WAL record", e);
        }
    }

    private void createDataWrite(byte[] key, byte[] value) throws IOException {
        out.writeInt(key.length);
        out.writeInt(value.length);
        out.write(key);
        out.write(value);
    }

    private int findNextIndex(Path dir) throws IOException {
        int max = 0;
        Pattern p = Pattern.compile("^wal_(\\d{4})$");
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, "wal_*")) {
            for (Path pth : ds) {
                String name = pth.getFileName().toString();
                Matcher m = p.matcher(name);
                if (m.matches()) {
                    try {
                        int v = Integer.parseInt(m.group(1));
                        if (v > max)
                            max = v;
                    } catch (NumberFormatException ignored) {
                    }
                }
            }
        }
        return max + 1;
    }

    private void openNewWalFile() throws IOException {
        String fileName = String.format("wal_%04d", currentIndex);
        Path currentWalFilePath = walDirPath.resolve(fileName);
        this.fileOutputStream = new FileOutputStream(currentWalFilePath.toFile(), true);
        BufferedOutputStream bos = new BufferedOutputStream(fileOutputStream, BUFFER_SIZE);
        this.out = new DataOutputStream(bos);

        if (this.totalWritten == null) {
            this.totalWritten = new AtomicLong(0);
        } else {
            this.totalWritten.set(0);
        }
        System.out.println("Opened WAL file: " + currentWalFilePath.getFileName());
    }

    private void rotateWalFile() throws IOException {
        try {
            if (out != null) {
                out.flush();
                out.close();
            }
        } catch (IOException e) {
            throw new IOException("Failed to close WAL file during rotation", e);
        }

        try {
            if (fileOutputStream != null)
                fileOutputStream.close();
        } catch (IOException e) {
            throw new IOException("Failed to close WAL file during rotation", e);
        }
        currentIndex++;
        openNewWalFile();
    }

    @Override
    public synchronized void close() throws IOException {
        if (out != null) {
            out.flush();
            out.close();
        }
    }
}
