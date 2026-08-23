package com.projects.rabey;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WALRecoveryManager {

    private final Path walDirPath;
    private final LSMTree memTable;

    public WALRecoveryManager(Path walDirPath, LSMTree lsmTree) {
        this.walDirPath = walDirPath;
        this.memTable = lsmTree;
    }

    public void RecoverData() {
        try {
            List<Path> walFiles = getSortedWalFiles(walDirPath);
            if (walFiles.isEmpty()) {
                return;
            }

            // Temporarily disable writing to WAL during state recovery
            memTable.setWalEnabled(false);
            try {
                for (Path walPath : walFiles) {
                    try (WALReader reader = new WALReader(walPath)) {
                        WALRecord record;
                        while ((record = reader.readNext()) != null) {
                            memTable.put(record.key(), record.value());
                        }
                    }
                }
            } finally {
                memTable.setWalEnabled(true);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to recover from WAL", e);
        }
    }

    private List<Path> getSortedWalFiles(Path dir) throws IOException {
        List<Path> walFiles = new ArrayList<>();
        if (!Files.exists(dir)) {
            return walFiles;
        }

        Pattern p = Pattern.compile("^wal_(\\d{4})$");
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, "wal_*")) {
            for (Path pth : ds) {
                Matcher m = p.matcher(pth.getFileName().toString());
                if (m.matches()) {
                    walFiles.add(pth);
                }
            }
        }

        // Sort files chronologically based on their numerical sequence index
        walFiles.sort(Comparator.comparingInt(path -> {
            Matcher m = p.matcher(path.getFileName().toString());
            return m.matches() ? Integer.parseInt(m.group(1)) : -1;
        }));

        return walFiles;
    }
}