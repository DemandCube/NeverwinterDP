package com.neverwinterdp.storage.hdfs.sink;

import java.io.IOException;

import com.neverwinterdp.ssm.hdfs.HdfsSSM;
import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.sink.SinkPartitionStream;
import com.neverwinterdp.storage.sink.SinkPartitionStreamWriter;

public class HDFSSinkPartitionStream implements SinkPartitionStream {
  private HdfsSSM                partitionStorage;
  private StorageConfig          storageConfig;
  private PartitionStreamConfig  partitionConfig;
  
  public HDFSSinkPartitionStream(HdfsSSM pStorage, StorageConfig sConfig, PartitionStreamConfig pConfig) throws IOException {
    this.partitionStorage  = pStorage;
    this.storageConfig     = sConfig;
    this.partitionConfig   = pConfig;
  }
  
  public int getPartitionStreamId() { return partitionConfig.getPartitionStreamId(); }
  
  public PartitionStreamConfig getParitionConfig() { return this.partitionConfig ; }
  
  @Override
  synchronized public SinkPartitionStreamWriter getWriter() throws IOException {
    return new HDFSSinkPartitionStreamWriter(partitionStorage, storageConfig, partitionConfig);
  }
  
  synchronized public void delete() throws Exception {
  }
  
  public void optimize() throws Exception {
  }
}