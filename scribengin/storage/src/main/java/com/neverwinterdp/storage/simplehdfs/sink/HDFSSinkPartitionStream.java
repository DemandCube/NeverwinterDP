package com.neverwinterdp.storage.simplehdfs.sink;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;

import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.sink.SinkPartitionStream;
import com.neverwinterdp.storage.sink.SinkPartitionStreamWriter;

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