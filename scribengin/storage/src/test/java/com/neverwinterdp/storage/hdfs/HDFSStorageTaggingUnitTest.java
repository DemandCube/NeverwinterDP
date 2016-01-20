package com.neverwinterdp.storage.hdfs;

import org.junit.Test;

public class HDFSStorageTaggingUnitTest extends HDFSStorageTest {
  @Test
  public void testHDFSStorage() throws Exception {
    int NUM_OF_STREAMS = 5;
    HDFSStorageConfig storageConfig = new HDFSStorageConfig("test", HDFS_DIR);
    storageConfig.setReplication(2);
    storageConfig.setPartitionStream(NUM_OF_STREAMS);
    HDFSStorage storage = new HDFSStorage(registry,fs, storageConfig);
    storage.create();
    
    write(storage, 1000 /*1000 message per stream*/);
    storage.doManagement();
    HDFSStorageTag tag100 = storage.getRegistry().createTagByPosition("tag-100", "Tag at pos 100", 100);
    
    int totalCount = count(storage, tag100); //count from tag100
    System.err.println("Total Count = " + totalCount);
  }
}
