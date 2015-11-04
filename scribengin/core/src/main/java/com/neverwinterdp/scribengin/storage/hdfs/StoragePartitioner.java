package com.neverwinterdp.scribengin.storage.hdfs;

public interface StoragePartitioner<T> {
  public Segment newSegment(Storage<T> storage);
}
