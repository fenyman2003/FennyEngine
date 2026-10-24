package com.projects.rabey;

import java.io.IOException;
import java.nio.file.Path;

public class SSTTableReaderManager {

    private final Path sstableDirPath;

    public SSTTableReaderManager(Path sstableDirPath) {
        this.sstableDirPath = sstableDirPath;
    }

    public byte[] get(byte[] searchKey, int currentIndex) throws IOException {
        for (int i = currentIndex; i >= 1; i--) {
            try (SSTTableReader reader = new SSTTableReader(getSSTableFileName(i))) {
                byte[] result = reader.pointSearch(searchKey);
                if (result != null) {
                    return result;
                }
            }
        }
        return null;
    }

    private Path getSSTableFileName(int index) {
        return sstableDirPath.resolve(String.format("sstable_%04d", index));
    }
}
