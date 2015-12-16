package com.neverwinterdp.scribengin.dataflow.api;

import com.neverwinterdp.storage.StorageConfig;

public class HDFSDataStream<T> extends DataStream<T>{
  public HDFSDataStream(String name, DataStreamType type) {
    super(name, type);
  }

  @Override
  public StorageConfig getStorageConfig() {
    return null;
  }
}
