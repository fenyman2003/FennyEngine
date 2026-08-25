package com.projects.rabey;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws Exception {
        Path walPath = Paths.get("src/main/resources/wal");
        Path sstablePath = Paths.get("src/main/resources/sstable");

        // 1. Clean up any previous test WAL file
        File file = walPath.toFile();
        if (file.exists()) {
            file.delete();
        }

        // 2. Initialize the storage engine with low threshold for testing
        WALWriter walWriter = new WALWriter(walPath);
        SSTTableWriter sstTableWriter = new SSTTableWriter(sstablePath);
        SSTTableReaderManager sstTableReaderManager = new SSTTableReaderManager(sstablePath);
        StorageEngine engine = new StorageEngine(walWriter, sstTableWriter, sstTableReaderManager);

        // WALRecoveryManager recoveryManager = new WALRecoveryManager(walPath, engine);
        // recoveryManager.RecoverData();

        int totalWrites = 10_000_000; // 10 Million writes
        int threadCount = 8;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        long startTime = System.currentTimeMillis();

        // 3. Fire concurrent writes to trigger MemTable rotations
        for (int i = 0; i < totalWrites; i++) {
            final int id = i;
            executor.submit(() -> {
                byte[] key = String.format("key_%08d", id).getBytes(StandardCharsets.UTF_8);
                byte[] val = String.format("val_%08d", id).getBytes(StandardCharsets.UTF_8);

                engine.put(key, val);
                IO.println("Active MemTable size: " + engine.size());
            });
        }

        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);

        long endTime = System.currentTimeMillis();
        System.out.println("Completed " + totalWrites + " writes in " + (endTime - startTime) + " ms");

        // 4. Verify data reads across active & immutable MemTables
        byte[] testKey = String.format("key_%08d", 4242).getBytes(StandardCharsets.UTF_8);
        byte[] result = engine.get(testKey);

        if (result != null) {
            System.out.println("Verification Success: Found key_00004242 -> "
                            + new String(result, StandardCharsets.UTF_8));
        } else {
            System.err.println("Verification Failed: Key not found!");
        }

        walWriter.close();
    }
}