package com.neverwinterdp.scribengin.storage.sink;

import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.PartitionDescriptor;

public interface Sink {
  public StorageDescriptor getDescriptor();
  
  public SinkPartitionStream  getStream(PartitionDescriptor descriptor) throws Exception ;
  
  public SinkPartitionStream[] getStreams();

  public void delete(SinkPartitionStream stream) throws Exception;

  public SinkPartitionStream newStream() throws Exception ;

  public void close() throws Exception ;
}
