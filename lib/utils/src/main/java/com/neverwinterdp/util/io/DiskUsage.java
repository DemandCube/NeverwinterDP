package com.neverwinterdp.util.io;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Paths;

public class DiskUsage {
  static final long M = 1024 * 1024;

  static void printFileStore(FileStore store) throws IOException {
    long total = store.getTotalSpace() / M;
    long used = (store.getTotalSpace() - store.getUnallocatedSpace()) / M;
    long avail = store.getUsableSpace() / M;

    String s = store.name();
    System.out.format("%-20s %12d %12d %12d\n", s, total, used, avail);
  }

  public static void main(String[] args) throws IOException {
    System.out.format("%-20s %12s %12s %12s\n", "Filesystem", "Total kbytes", "used", "avail");
    if (args.length == 0) {
      FileSystem fs = FileSystems.getDefault();
      for (FileStore store: fs.getFileStores()) {
        printFileStore(store);
      }
    } else {
      for (String file: args) {
        FileStore store = Files.getFileStore(Paths.get(file));
        printFileStore(store);
      }
    }
  }
}
