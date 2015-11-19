package com.neverwinterdp.nstorage;

public class NStoragePartitionSegmentWriter<T> {
  private NStorageContext<T>       context;
  private NStoragePartition        partition;
  private NStoragePartitionSegment segment;
  private long                     maxPartitionSegmentSize;
  private long                     currentPartitionSegmentSize;

  public NStoragePartitionSegmentWriter(NStorageContext<T> context, NStoragePartition  partition, NStoragePartitionSegment segment) {
    this.context   = context;
    this.partition = partition ;
    this.segment =  segment;
    this.maxPartitionSegmentSize = context.getHQueue().getMaxPartitionSegmentSize();
    this.currentPartitionSegmentSize = segment.getAvailable();
  }
  
  public boolean isFull() { return currentPartitionSegmentSize >= maxPartitionSegmentSize; }
  
  public void append(T obj) throws NStorageException {
  }
  
  public void append(byte[] bytes) throws NStorageException {
    currentPartitionSegmentSize += bytes.length;
  }
  
  public void commit() throws NStorageException {
  }

  public void rollback() throws NStorageException {
  }
  
  public void close() throws NStorageException {
  }
}