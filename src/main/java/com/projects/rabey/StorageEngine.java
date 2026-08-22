package com.projects.rabey;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class StorageEngine {

    static void main() throws InterruptedException, IOException {
        Path path = Paths.get("wal");
        WALWriter walWriter = new WALWriter(path);
        LSMTree memTable = new LSMTree(walWriter);
    }
}
