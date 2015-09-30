package com.neverwinterdp.scribengin.storage.sink;

import com.neverwinterdp.scribengin.storage.PartitionDescriptor;

public interface SinkPartitionStream {
  public PartitionDescriptor getDescriptor();
  public void delete() throws Exception;
  public SinkPartitionStreamWriter getWriter() throws Exception ;
  public void optimize() throws Exception ;
}
