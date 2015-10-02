package com.neverwinterdp.hqueue;

public class HQueuePartitionSegmentWriter<T> {
  private HQueueContext<T>       context;
  private HQueuePartition        partition;
  private HQueuePartitionSegment segment;
  private long                   maxPartitionSegmentSize;
  private long                   currentPartitionSegmentSize;
  
  public HQueuePartitionSegmentWriter(HQueueContext<T> context, HQueuePartition  partition, HQueuePartitionSegment segment) {
    this.context   = context;
    this.partition = partition ;
    this.segment =  segment;
    this.maxPartitionSegmentSize = context.getHQueue().getMaxPartitionSegmentSize();
    this.currentPartitionSegmentSize = segment.getAvailable();
  }
  
  public boolean isFull() { return currentPartitionSegmentSize >= maxPartitionSegmentSize; }
  
  public void append(T obj) throws HQueueException {
  }
  
  public void append(byte[] bytes) throws HQueueException {
    currentPartitionSegmentSize += bytes.length;
  }
  
  public void commit() throws HQueueException {
  }

  public void rollback() throws HQueueException {
  }
  
  public void close() throws HQueueException {
  }
}