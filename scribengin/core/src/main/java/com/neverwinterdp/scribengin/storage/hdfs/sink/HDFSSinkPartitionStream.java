package com.neverwinterdp.scribengin.storage.hdfs.sink;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;

import com.neverwinterdp.scribengin.storage.PartitionStreamConfig;
import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStream;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStreamWriter;

public class HDFSSinkPartitionStream implements SinkPartitionStream {
  private FileSystem             fs;
  private StorageConfig          storageConfig;
  private PartitionStreamConfig  partitionConfig;
  
  public HDFSSinkPartitionStream(FileSystem fs, StorageConfig sConfig, PartitionStreamConfig pConfig) throws IOException {
    this.fs = fs;
    this.storageConfig = sConfig;
    this.partitionConfig = pConfig;
  }
  
  public int getPartitionStreamId() { return partitionConfig.getPartitionStreamId(); }
  
  public PartitionStreamConfig getParitionConfig() { return this.partitionConfig ; }
  
  @Override
  synchronized public SinkPartitionStreamWriter getWriter() throws IOException {
    return new HDFSSinkPartitionStreamWriter(fs, storageConfig, partitionConfig);
  }
  
  synchronized public void delete() throws Exception {
  }
  
  public void optimize() throws Exception {
    //storage.refresh();
    //storage.optimizeBufferSegments();
  }
}