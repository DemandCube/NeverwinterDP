package com.neverwinterdp.scribengin.storage.hdfs;

public interface OperationExecutor<T> {
  public void execute(Storage<T> storage, Lock lock, OperationConfig config) throws Exception ;
}
