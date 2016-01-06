package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.hdfs.HDFSStorageConfig;

public class HDFSDataSet<T> extends DataSet<T>{
  private HDFSStorageConfig hdfsStorageConfig;
  
  public HDFSDataSet(DataStreamType type, HDFSStorageConfig hdfsStorageConfig) {
    super(hdfsStorageConfig.getName(), type);
    this.hdfsStorageConfig = hdfsStorageConfig;
  }
  
  public HDFSDataSet(String name, DataStreamType type, String registryPath, String location) {
    super(name, type);
    hdfsStorageConfig = new HDFSStorageConfig(name, registryPath, location);
    hdfsStorageConfig.setName(name);
  }

  @Override
  protected StorageConfig createStorageConfig() { 
    return new HDFSStorageConfig(hdfsStorageConfig); 
  }
}
