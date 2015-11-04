package com.neverwinterdp.scribengin.storage.sink;

import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.PartitionStreamConfig;

public interface Sink {
  public StorageConfig getDescriptor();
  
  public SinkPartitionStream  getPartitionStream(PartitionStreamConfig descriptor) throws Exception ;
  
  public SinkPartitionStream  getParitionStream(int partitionId) throws Exception ;
  
  public SinkPartitionStream[] getPartitionStreams();

  public void delete(SinkPartitionStream stream) throws Exception;

  public SinkPartitionStream newStream() throws Exception ;

  public void close() throws Exception ;
}
