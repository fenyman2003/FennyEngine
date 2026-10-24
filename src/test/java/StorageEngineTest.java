import com.projects.rabey.SSTTableReaderManager;
import com.projects.rabey.SSTTableWriter;
import com.projects.rabey.StorageEngine;
import com.projects.rabey.WALWriter;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.file.DirectoryStream;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class StorageEngineTest {

    private static final int TEST_SIZE = 100_000; // 1 Million writes for stress testing
    private static Path walPath = Paths.get("src/test/wal");
    private static Path sstablePath = Paths.get("src/test/sstable");

    @BeforeEach
    public void init() throws IOException {
        deleteDirectory(walPath.toFile());
        deleteDirectory(sstablePath.toFile());
        IO.println("WAL dir: " + walPath.toAbsolutePath().toString());
        IO.println("SSTable dir: " + sstablePath.toAbsolutePath().toString());
    }

    @AfterAll
    public static void postAll() throws IOException {
        deleteDirectory(walPath.toFile());
        deleteDirectory(sstablePath.toFile());
        IO.println(walPath.toAbsolutePath().toString());
    }

    @Test
    public void test_storage_engine() throws Exception {

        WALWriter walWriter = new WALWriter(walPath);
        SSTTableWriter sstTableWriter = new SSTTableWriter(sstablePath);
        SSTTableReaderManager sstTableReaderManager = new SSTTableReaderManager(sstablePath);
        StorageEngine storageEngine = new StorageEngine(walWriter, sstTableWriter, sstTableReaderManager, 1024 * 1024);
        ExecutorService writerPool = Executors.newFixedThreadPool(8);

        StopWatch watch = new StopWatch();
        watch.start();

        for (int i = 0; i < TEST_SIZE; i++) {
            final int index = i;
            if (i % 10_000 == 0)
                IO.println(i + " records processed");
            writerPool.submit(() -> {
                storageEngine.put(("key" + index).getBytes(), ("value" + index).getBytes());
            });
        }
        // Shut down writer pool and wait (give it a reasonable timeout)
        writerPool.shutdown();
        if (writerPool.awaitTermination(10, TimeUnit.SECONDS)) {
            walWriter.close();
            watch.stop();
            IO.println("Time elapsed: " + watch.getTime() + " ms");
        } else {
            IO.println("Writer pool did not terminate in time.");
        }

        int sstableCount = countFiles(sstablePath, "sstable_*");
        int walCount = countFiles(walPath, "wal_*");
        IO.println("WAL files count: " + walCount + ", SSTable files count: " + sstableCount);
        Assertions.assertTrue(walCount > 0, "No WAL files created.");
        Assertions.assertTrue(sstableCount > 0, "No SSTable files created.");

        // Bounded reader pool with back-pressure (CallerRunsPolicy) and error capture
        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        ThreadPoolExecutor readerPool = new ThreadPoolExecutor(8, 8, 0L, TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

        watch.reset();
        watch.start();

        // Sample reads: pick 10_000 keys uniformly at random from the written keyspace
        // [0, TEST_SIZE)
        Random rnd = new Random(0);
        final int SAMPLE_COUNT = 100;
        for (int s = 0; s < SAMPLE_COUNT; s++) {
            final int idx = rnd.nextInt(100); // 0.TEST_SIZE-1
            IO.println(idx);
            readerPool.submit(() -> {
                try {
                    String got = new String(storageEngine.get(("key" + idx).getBytes()), StandardCharsets.UTF_8);
                    if (!("value" + idx).equals(got)) {
                        errors.add(new AssertionError("Incorrect value read for key" + idx + ", got=" + got));
                    }
                } catch (Throwable t) {
                    errors.add(t);
                }
            });
        }

        readerPool.shutdown();
        if (readerPool.awaitTermination(5, TimeUnit.MINUTES)) {
            watch.stop();
            IO.println("Time elapsed: " + watch.getTime() + " ms");
        } else {
            IO.println("reader pool did not terminate in time.");
        }

        if (!errors.isEmpty()) {
            Throwable t = errors.peek();
            if (t instanceof AssertionError)
                throw (AssertionError) t;
            else
                throw new RuntimeException("Reader tasks failed", t);
        }

        IO.println(storageEngine.size() + " entries in the map.");
        IO.println(calculateBytes() + " bytes expected for " + TEST_SIZE + " entries.");
    }

    private static int countFiles(Path dir, String glob) throws IOException {
        int count = 0;
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, glob)) {
            for (Path p : ds)
                count++;
        }
        return count;
    }

    private static void deleteDirectory(File directory) {
        // If it's a directory, empty its contents first
        if (directory.isDirectory()) {
            File[] allContents = directory.listFiles();
            if (allContents != null) {
                for (File file : allContents) {
                    // Recursive call to handle nested folders/files
                    deleteDirectory(file);
                }
            }
        }
        // Delete the file or the now-empty directory
        directory.delete();
    }

    private static long calculateBytes() {
        long totalBytes = 0;
        for (int i = 0; i < TEST_SIZE; i++) {
            String key = "key" + i;
            String value = "value" + i;
            totalBytes += key.getBytes(StandardCharsets.UTF_8).length + value.getBytes(StandardCharsets.UTF_8).length
                            + 8; // 8 bytes for the entry overhead
        }
        return totalBytes;
    }
}
