package com.neverwinterdp.storage.hdfs;

import java.util.Date;

import org.junit.Test;

import com.neverwinterdp.util.JSONSerializer;

public class HDFSStorageTaggingUnitTest extends HDFSStorageTest {
  @Test
  public void testTaggingByPosition() throws Exception {
    int NUM_OF_STREAMS = 5;
    HDFSStorageConfig storageConfig = new HDFSStorageConfig("test", HDFS_DIR);
    storageConfig.setReplication(2);
    storageConfig.setPartitionStream(NUM_OF_STREAMS);
    HDFSStorage storage = new HDFSStorage(registry,fs, storageConfig);
    storage.create();
    
    write(storage, 1000 /*1000 message per stream*/);
    storage.doManagement();
    
    HDFSStorageTag latestRecorPosTag = storage.findTagByRecordLastPosition("latest-record-pos-tag", "get the tag at last posistion");
    System.err.println("Latest Record Pos Tag: ");
    System.err.println(JSONSerializer.INSTANCE.toString(latestRecorPosTag));
    
    HDFSStorageTag tag100 = storage.findTagByPosition("tag-100", "Tag at pos 100", 100);
    storage.createTag(tag100);
    System.err.println("Tag At 100: ");
    System.err.println(JSONSerializer.INSTANCE.toString(tag100));
    
    
    int totalCount = count(storage, tag100); //count from tag100
    System.err.println("Total Count = " + totalCount);
  }
  
  @Test
  public void testTaggingByDate() throws Exception {
    int NUM_OF_STREAMS = 5;
    HDFSStorageConfig storageConfig = new HDFSStorageConfig("test", HDFS_DIR);
    storageConfig.setReplication(2);
    storageConfig.setPartitionStream(NUM_OF_STREAMS);
    HDFSStorage storage = new HDFSStorage(registry,fs, storageConfig);
    storage.create();
    
    write(storage, 1000 /*1000 message per stream*/);
    storage.doManagement();
    
    write(storage, 1000 /*1000 message per stream*/);
    storage.doManagement();
    
    HDFSStorageTag time1Tag = storage.findTagByDateTime("time-1", "Tag by the current time1", new Date());
    storage.createTag(time1Tag);
    
    System.err.println("Tag At  Time 1: ");
    System.err.println(JSONSerializer.INSTANCE.toString(time1Tag));
    
    write(storage, 1000 /*1000 message per stream*/);
    storage.doManagement();
    
    int totalCount = count(storage, time1Tag); //count from tag100
    System.err.println("Total Count = " + totalCount);
  }
}
