package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.hdfs.HDFSStorageConfig;

public class HDFSDataSet<T> extends DataSet<T>{
  private HDFSStorageConfig hdfsStorageConfig;
  
  public HDFSDataSet(DataStreamType type, HDFSStorageConfig hdfsStorageConfig) {
    super(hdfsStorageConfig.getName(), type);
    this.hdfsStorageConfig = hdfsStorageConfig;
  }
  
  @Override
  protected StorageConfig createStorageConfig() { 
    return new HDFSStorageConfig(hdfsStorageConfig); 
  }
}
