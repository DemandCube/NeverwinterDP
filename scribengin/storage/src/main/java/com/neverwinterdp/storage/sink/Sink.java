package com.neverwinterdp.storage.sink;

import java.util.List;

import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.StorageConfig;

public interface Sink {
  public StorageConfig getStorageConfig();
  
  public List<PartitionStreamConfig> getPartitionStreamConfigs() throws Exception ;
  
  public SinkPartitionStream  getPartitionStream(PartitionStreamConfig descriptor) throws Exception ;
  
  public SinkPartitionStream  getParitionStream(int partitionId) throws Exception ;
  
  public SinkPartitionStream[] getPartitionStreams() throws Exception ;

  public void close() throws Exception ;
}
