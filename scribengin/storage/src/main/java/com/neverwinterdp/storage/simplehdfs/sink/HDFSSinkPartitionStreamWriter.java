package com.neverwinterdp.storage.simplehdfs.sink;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.simplehdfs.HDFSStoragePartitioner;
import com.neverwinterdp.storage.simplehdfs.Segment;
import com.neverwinterdp.storage.simplehdfs.SegmentStorage;
import com.neverwinterdp.storage.simplehdfs.SegmentStorageWriter;
import com.neverwinterdp.storage.sink.SinkPartitionStreamWriter;

public class HDFSSinkPartitionStreamWriter implements SinkPartitionStreamWriter {
  private FileSystem                   fs;
  private StorageConfig                storageConfig;
  private PartitionStreamConfig        partitionConfig;
  private HDFSStoragePartitioner       partitioner;
  
  private String                       currentPartition;
  private SegmentStorageWriter<Message> writer;
  private long                         smallDataSizeAccumulate  = 0;
  private long                         mediumDataSizeAccumulate = 0;

  public HDFSSinkPartitionStreamWriter(FileSystem fs, StorageConfig sConfig, PartitionStreamConfig pConfig) throws IOException {
    this.fs = fs;
    this.storageConfig = sConfig;
    this.partitionConfig = pConfig ;
    this.partitioner = getPartitioner(sConfig);
  }
  
  public PartitionStreamConfig getPartitionConfig() { return partitionConfig; }
  
  @Override
  public void append(Message obj) throws Exception {
    if(writer == null) createWriter();
    writer.append(obj);
    long dataSize = obj.getData().length + obj.getKey().length() ;
    smallDataSizeAccumulate  += dataSize ;
    mediumDataSizeAccumulate += dataSize ;
  }

  @Override
  public void prepareCommit() throws Exception {
    if(writer == null) return;
    writer.prepareCommit();
  }
  
  @Override
  public void completeCommit() throws Exception {
    if(writer == null) return;
    writer.completeCommit();
    if(smallDataSizeAccumulate >= Segment.SMALL_DATASIZE_THRESHOLD) {
      writer.getStorage().optimizeBufferSegments();
      smallDataSizeAccumulate = 0 ;
    }
    if(mediumDataSizeAccumulate >= Segment.MEDIUM_DATASIZE_THRESHOLD) {
      writer.getStorage().optimizeSmallSegments();
      mediumDataSizeAccumulate = 0 ;
    }
    if(!partitioner.getCurrentPartition().equals(currentPartition)) {
      close();
    }
  }

  @Override
  public void commit() throws Exception {
    if(writer == null) return;
    prepareCommit();
    completeCommit();
  }
  
  @Override
  public void rollback() throws Exception {
    if(writer == null) return;
    writer.rollback();
  }

  @Override
  public void close() throws Exception {
    if(writer == null) return;
    writer.close();
    writer = null;
  }
  
  private void createWriter() throws Exception {
    int partitionStreamId = partitionConfig.getPartitionStreamId();
    currentPartition = partitioner.getCurrentPartition();
    String streamLoc = storageConfig.getLocation() + "/" + currentPartition + "/partition-stream-" + partitionStreamId;
    SegmentStorage<Message> storage = new SegmentStorage<>(fs, streamLoc, Message.class);
    writer = new SegmentStorageWriter<Message>(storage);
    smallDataSizeAccumulate  = storage.getBufferSegments().dataSize();
    mediumDataSizeAccumulate = storage.getSmallSegments().dataSize();
  }
  
  HDFSStoragePartitioner getPartitioner(StorageConfig sConfig) {
    HDFSStoragePartitioner partitioner = new HDFSStoragePartitioner.Default();
    String name = sConfig.attribute("partitioner");
    if("hourly".equals(name)) {
      partitioner = new HDFSStoragePartitioner.Hourly();
    } else if("15min".equals(name)) {
      partitioner = new HDFSStoragePartitioner.Every15Min();
    }
    return partitioner;
  }
}