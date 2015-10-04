package com.neverwinterdp.scribengin.storage.sink;

import com.neverwinterdp.scribengin.storage.PartitionConfig;

public interface SinkPartitionStream {
  public PartitionConfig getParitionConfig();
  public void delete() throws Exception;
  public SinkPartitionStreamWriter getWriter() throws Exception ;
  public void optimize() throws Exception ;
}
