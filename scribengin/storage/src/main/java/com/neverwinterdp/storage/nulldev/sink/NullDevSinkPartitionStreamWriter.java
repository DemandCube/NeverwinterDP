package com.neverwinterdp.storage.nulldev.sink;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.sink.SinkPartitionStreamWriter;

public class NullDevSinkPartitionStreamWriter implements SinkPartitionStreamWriter {
  private StorageConfig         storageConfig;
  private PartitionStreamConfig partitionStreamConfig;
  
  public NullDevSinkPartitionStreamWriter(StorageConfig storageConfig, PartitionStreamConfig partitionStreamConfig) {
    this.storageConfig         = storageConfig;
    this.partitionStreamConfig = partitionStreamConfig;
  }
  
  @Override
  public void append(Message record) throws Exception {
  }

  @Override
  public void close() throws Exception {
  }

  @Override
  public void rollback() throws Exception {
  }

  @Override
  public void commit() throws Exception {
  }

  @Override
  public void prepareCommit() throws Exception {
  }

  @Override
  public void completeCommit() {
  }
}