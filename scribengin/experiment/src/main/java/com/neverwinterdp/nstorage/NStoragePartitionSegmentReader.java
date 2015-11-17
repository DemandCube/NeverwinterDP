package com.neverwinterdp.nstorage;

public class NStoragePartitionSegmentReader<T> {
  private NStorageContext<T> context;
  private NStoragePartition  partition ;
  
  private String name ;
  private long   currentReadPos = 0;
  
  public NStoragePartitionSegmentReader(NStorageContext<T> context, NStoragePartition  partition) {
    this.context   = context;
    this.partition = partition;
  }
  
  public T next(long maxWaitTime) throws NStorageException {
    return null;
  }

  public void commit() throws NStorageException {
  }

  public void rollback() throws NStorageException {
  }

  
  public void seek(long pos) throws NStorageException {
  }
  
  public void close() throws NStorageException {
  }
}