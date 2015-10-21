package com.neverwinterdp.hqueue;

public class HQueuePartitionSegmentReader<T> {
  private HQueueContext<T> context;
  private HQueuePartition  partition ;
  
  private String name ;
  private long   currentReadPos = 0;
  
  public HQueuePartitionSegmentReader(HQueueContext<T> context, HQueuePartition  partition) {
    this.context   = context;
    this.partition = partition;
  }
  
  public T next(long maxWaitTime) throws HQueueException {
    return null;
  }

  public void commit() throws HQueueException {
  }

  public void rollback() throws HQueueException {
  }

  
  public void seek(long pos) throws HQueueException {
  }
  
  public void close() throws HQueueException {
  }
}