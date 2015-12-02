package com.neverwinterdp.storage.simplehdfs;

public interface SegmentOperationExecutor<T> {
  public void execute(SegmentStorage<T> storage, SegmentLock lock, SegmentOperationConfig config) throws Exception ;
}
