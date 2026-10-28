package com.projects.rabey;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SSTTableWriter implements AutoCloseable {

    public static Path sstableDirPath = null;
    private static final int BUFFER_SIZE = 8 * 1024; // 8KB buffer size
    private volatile int lastDurableIndex;

    // Construct with directory where sstable files (sstable_0001, sstable_0002...)
    // are stored
    public SSTTableWriter(Path sstableDirPath) throws IOException {
        this.sstableDirPath = sstableDirPath;
        if (!Files.exists(sstableDirPath)) {
            Files.createDirectories(sstableDirPath);
        }
        deleteStaleTempFiles(sstableDirPath);
        this.lastDurableIndex = findNextIndex(sstableDirPath) - 1;
    }

    /**
     * Writes all entries to a single SSTable. The table is written to a temporary
     * file, flushed and fsynced, and only then atomically published under its final
     * name. Readers therefore only ever observe complete, durable tables.
     */
    public synchronized void writeSSTable(Iterable<Map.Entry<byte[], byte[]>> entries) throws IOException {
        Iterator<Map.Entry<byte[], byte[]>> iterator = entries.iterator();
        if (!iterator.hasNext()) {
            return;
        }

        int index = lastDurableIndex + 1;
        Path tempPath = sstablePath(String.format("sstable_%04d.tmp", index));
        Path finalPath = sstablePath(String.format("sstable_%04d", index));

        try (FileOutputStream fileOutputStream = new FileOutputStream(tempPath.toFile(), false);
                        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream,
                                        BUFFER_SIZE);
                        DataOutputStream out = new DataOutputStream(bufferedOutputStream)) {
            while (iterator.hasNext()) {
                Map.Entry<byte[], byte[]> entry = iterator.next();
                writeRecord(out, entry.getKey(), entry.getValue());
            }

            // Flush Java buffers to the OS page cache
            out.flush();

            // Force OS page cache to physical storage media
            fileOutputStream.getFD().sync();

        } catch (IOException | RuntimeException e) {
            try {
                Files.deleteIfExists(tempPath);
            } catch (IOException ignored) {
            }
            throw e;
        }

        try {
            moveIntoPlace(tempPath, finalPath);
        } catch (IOException e) {
            try {
                Files.deleteIfExists(tempPath);
            } catch (IOException ignored) {
            }
            throw e;
        }
        lastDurableIndex = index;
    }

    private void writeRecord(DataOutputStream out, byte[] key, byte[] value) throws IOException {
        if (key == null || value == null) {
            throw new IllegalArgumentException("Key and Value cannot be null");
        }
        out.writeInt(key.length);
        out.writeInt(value.length);
        out.write(key);
        out.write(value);
    }

    private Path sstablePath(String name) {
        return sstableDirPath.resolve(name);
    }

    private void moveIntoPlace(Path tempPath, Path finalPath) throws IOException {
        try {
            Files.move(tempPath, finalPath, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(tempPath, finalPath, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static void deleteStaleTempFiles(Path dir) throws IOException {
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, "sstable_*.tmp")) {
            for (Path path : ds) {
                Files.deleteIfExists(path);
            }
        }
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

    public int getSSTableFileIndex() {
        return this.lastDurableIndex;
    }

    @Override
    public void close() {
        // Each SSTable is closed as soon as it is written, so there is nothing to
        // release.
    }
}
