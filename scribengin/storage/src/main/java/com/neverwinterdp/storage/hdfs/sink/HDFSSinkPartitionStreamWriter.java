package com.neverwinterdp.storage.hdfs.sink;

import java.io.IOException;

import com.neverwinterdp.ssm.hdfs.HdfsSSM;
import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.Record;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.sink.SinkPartitionStreamWriter;

public class HDFSSinkPartitionStreamWriter implements SinkPartitionStreamWriter {
  private HdfsSSM                      partitionStorage;
  
  private StorageConfig                storageConfig;
  private PartitionStreamConfig        partitionConfig;

  public HDFSSinkPartitionStreamWriter(HdfsSSM pStorage, StorageConfig sConfig, PartitionStreamConfig pConfig) throws IOException {
    this.partitionStorage = pStorage;
    this.storageConfig    = sConfig;
    this.partitionConfig  = pConfig ;
  }
  
  public PartitionStreamConfig getPartitionConfig() { return partitionConfig; }

  @Override
  public void append(Record dataflowMessage) throws Exception {
  }

  @Override
  public void commit() throws Exception {
  }

  @Override
  public void close() throws Exception {
  }

  @Override
  public void rollback() throws Exception {
  }

  @Override
  public void prepareCommit() throws Exception {
  }

  @Override
  public void completeCommit() throws Exception {
  }
}