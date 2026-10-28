package com.projects.rabey;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class SSTTableReaderManager {

    private final Path sstableDirPath;

    public SSTTableReaderManager(Path sstableDirPath) {
        this.sstableDirPath = sstableDirPath;
    }

    public byte[] get(byte[] searchKey, int lastDurableIndex) throws IOException {
        for (int i = lastDurableIndex; i >= 1; i--) {
            Path sstablePath = getSSTableFileName(i);
            if (!Files.exists(sstablePath)) {
                // Gaps are expected when a flush failed; skip rather than abort the lookup.
                continue;
            }
            try (SSTTableReader reader = new SSTTableReader(sstablePath)) {
                byte[] result = reader.pointSearch(searchKey);
                if (result != null) {
                    return result;
                }
            } catch (FileNotFoundException e) {
                // The file disappeared between the existence check and opening it.
            }
        }
        return null;
    }

    private Path getSSTableFileName(int index) {
        return sstableDirPath.resolve(String.format("sstable_%04d", index));
    }
}
