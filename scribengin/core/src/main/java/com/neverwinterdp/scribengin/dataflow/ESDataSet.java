package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.es.ESStorageConfig;

public class ESDataSet<T> extends DataSet<T> {
  private ESStorageConfig esStorageConfig;

  public ESDataSet(DataStreamType type, ESStorageConfig esStorageConfig) {
    super(esStorageConfig.getName(), type);
    this.esStorageConfig = esStorageConfig;
  }

  @Override
  protected StorageConfig createStorageConfig() { return new ESStorageConfig(esStorageConfig); }

}
