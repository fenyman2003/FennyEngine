package com.projects.rabey;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SSTTableWriter implements AutoCloseable {

    public static Path sstableDirPath = null;
    private static final int BUFFER_SIZE = 8 * 1024; // 8KB buffer size
    private DataOutputStream out;
    private FileOutputStream fileOutputStream;
    private AtomicInteger currentIndex = new AtomicInteger(0);

    // Construct with directory where sstable files (sstable_0001, sstable_0002...)
    // are stored
    public SSTTableWriter(Path sstableDirPath) throws IOException {
        this.sstableDirPath = sstableDirPath;
        if (!Files.exists(sstableDirPath)) {
            Files.createDirectories(sstableDirPath);
        }
        this.currentIndex.set(findNextIndex(sstableDirPath));
    }

    // Append key length | value length | key | value to the SSTable file
    public synchronized void write(byte[] key, byte[] value) {
        if (key == null || value == null) {
            throw new IllegalArgumentException("Key and Value cannot be null");
        }

        try {
            createDataWrite(key, value);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write SSTable record", e);
        }
    }

    private void createDataWrite(byte[] key, byte[] value) throws IOException {
        out.writeInt(key.length);
        out.writeInt(value.length);
        out.write(key);
        out.write(value);
    }

    public static int findNextIndex(Path dir) throws IOException {
        int max = 0;
        Pattern p = Pattern.compile("^sstable_(\\d{4})$");
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, "sstable_*")) {
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

    private void openNewSSTTableFile() throws IOException {
        String fileName = String.format("sstable_%04d", currentIndex);
        Path currentSSTTableFilePath = sstableDirPath.resolve(fileName);
        this.fileOutputStream = new FileOutputStream(currentSSTTableFilePath.toFile(), true);
        BufferedOutputStream bos = new BufferedOutputStream(fileOutputStream, BUFFER_SIZE);
        this.out = new DataOutputStream(bos);
        System.out.println("Opened SSTable file: " + currentSSTTableFilePath.getFileName());
    }

    public void createNewSSTTableFile() throws IOException {
        try {
            if (out != null) {
                out.flush();
                out.close();
            }
        } catch (IOException e) {
            throw new IOException("Failed to close SSTable file during rotation", e);
        }

        try {
            if (fileOutputStream != null)
                fileOutputStream.close();
        } catch (IOException e) {
            throw new IOException("Failed to close SSTable file during rotation", e);
        }
        openNewSSTTableFile();
        currentIndex.addAndGet(1);
    }

    public int getSSTableFileIndex(){
        return this.currentIndex.get();
    }

    @Override
    public synchronized void close() throws IOException {
        if (out != null) {
            out.flush();
            out.close();
        }
    }
}
