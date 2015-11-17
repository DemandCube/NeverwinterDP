package com.neverwinterdp.scribengin.storage.hdfs;

public interface SegmentOperationExecutor<T> {
  public void execute(SegmentStorage<T> storage, SegmentLock lock, SegmentOperationConfig config) throws Exception ;
}
