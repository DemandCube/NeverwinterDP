package com.neverwinterdp.scribengin.storage.sink;

import com.neverwinterdp.scribengin.storage.StorageConfig;

import java.util.List;

import com.neverwinterdp.scribengin.storage.PartitionStreamConfig;

public interface Sink {
  public StorageConfig getDescriptor();
  
  public List<PartitionStreamConfig> getPartitionStreamConfigs() throws Exception ;
  
  public SinkPartitionStream  getPartitionStream(PartitionStreamConfig descriptor) throws Exception ;
  
  public SinkPartitionStream  getParitionStream(int partitionId) throws Exception ;
  
  public SinkPartitionStream[] getPartitionStreams() throws Exception ;

  public void close() throws Exception ;
}
