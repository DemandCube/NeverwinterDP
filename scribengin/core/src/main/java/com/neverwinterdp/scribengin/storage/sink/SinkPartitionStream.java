package com.neverwinterdp.scribengin.storage.sink;

public interface SinkPartitionStream {
  public int getPartitionStreamId() ;
  public void delete() throws Exception;
  public SinkPartitionStreamWriter getWriter() throws Exception ;
  public void optimize() throws Exception ;
}
