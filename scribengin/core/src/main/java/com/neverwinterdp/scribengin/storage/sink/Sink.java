package com.neverwinterdp.scribengin.storage.sink;

import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.PartitionConfig;

public interface Sink {
  public StorageConfig getDescriptor();
  
  public SinkPartitionStream  getStream(PartitionConfig descriptor) throws Exception ;
  
  public SinkPartitionStream  getStream(int partitionId) throws Exception ;
  
  public SinkPartitionStream[] getStreams();

  public void delete(SinkPartitionStream stream) throws Exception;

  public SinkPartitionStream newStream() throws Exception ;

  public void close() throws Exception ;
}
