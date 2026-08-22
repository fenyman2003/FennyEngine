import com.projects.rabey.LSMTree;
import com.projects.rabey.WALWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class StorageEngineTest {

    private static final int TEST_SIZE = 100000;

    @BeforeAll
    public static void init() throws IOException {
        Path path = Paths.get("wal");
        File file = path.toFile();
        deleteDirectory(file);
        IO.println(path.toAbsolutePath().toString());
    }

    @Test
    public void test_walWrite_and_lsm_write_behaviour() throws InterruptedException, IOException {
        Path path = Paths.get("wal");

        WALWriter walWriter = new WALWriter(path);
        LSMTree memTable = new LSMTree(walWriter);
        ExecutorService writerPool = Executors.newFixedThreadPool(8);

        StopWatch watch = new StopWatch();
        watch.start();

        for (int i = 0; i < TEST_SIZE; i++) {
            final int index = i;
            writerPool.submit(() -> {
                memTable.put(("key" + index).getBytes(), ("value" + index).getBytes());
            });
        }
        // Shut down writer pool and wait (give it a reasonable timeout)
        writerPool.shutdown();
        if (writerPool.awaitTermination(10, TimeUnit.SECONDS)) {
            walWriter.close();
            watch.stop();
            System.out.println("Time elapsed: " + watch.getTime() + " ms");
        } else {
            System.out.println("Writer pool did not terminate in time.");
        }

        IO.println(memTable.size() + " entries in the map.");
        // Iterate over the map and print the key-value pairs
        watch.reset();
        watch.start();
        AtomicInteger counter = new AtomicInteger(0);

        // Use a separate pool for reads so we don't submit to a shutdown executor
        ExecutorService readerPool = Executors.newFixedThreadPool(8);
        for (int i = TEST_SIZE - 1; i >= 0; i--) {
            final int index = i;
            int finalI = i;
            readerPool.submit(() -> {
                String result = new String(memTable.get(("key" + index).getBytes()), StandardCharsets.UTF_8);
                // IO.println("index : " + finalI + " value : " + result);
                counter.addAndGet(1);
            });
        }
        readerPool.shutdown();
        if (readerPool.awaitTermination(10, TimeUnit.SECONDS)) {
            watch.stop();
            System.out.println("Time elapsed: " + watch.getTime() + " ms");
        } else {
            System.out.println("Reader pool did not terminate in time.");
        }

        IO.println("Total entries retrieved: " + counter.get());
        IO.println(FileUtils.sizeOfDirectory(path.toFile()) + " bytes in WAL directory.");
        IO.println(calculateBytes() + " bytes expected for " + TEST_SIZE + " entries.");
        Assertions.assertEquals(calculateBytes(), FileUtils.sizeOfDirectory(path.toFile()),
                        "WAL directory size does not match expected bytes.");
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
