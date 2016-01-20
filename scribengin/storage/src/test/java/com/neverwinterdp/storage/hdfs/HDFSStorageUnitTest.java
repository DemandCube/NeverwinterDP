package com.neverwinterdp.storage.hdfs;

import org.junit.Assert;
import org.junit.Test;

public class HDFSStorageUnitTest extends HDFSStorageTest {
  @Test
  public void testHDFSStorage() throws Exception {
    int NUM_OF_STREAMS = 5;
    HDFSStorageConfig storageConfig = new HDFSStorageConfig("test", HDFS_DIR);
    storageConfig.setReplication(2);
    storageConfig.setPartitionStream(NUM_OF_STREAMS);
    HDFSStorage storage = new HDFSStorage(registry,fs, storageConfig);
    storage.create();
    registry.get("/").dump(System.out);
    write(storage, 1000 /*1000 message per stream*/);

    int totalCount = count(storage);
    Assert.assertEquals(NUM_OF_STREAMS * 1000, totalCount);
  }
}
