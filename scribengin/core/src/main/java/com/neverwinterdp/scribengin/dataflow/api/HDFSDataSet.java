package com.neverwinterdp.scribengin.dataflow.api;

import com.neverwinterdp.storage.StorageConfig;

public class HDFSDataSet<T> extends DataSet<T>{
  public HDFSDataSet(String name, DataSetType type) {
    super(name, type);
  }

  @Override
  public StorageConfig getStorageConfig() {
    return null;
  }
}
