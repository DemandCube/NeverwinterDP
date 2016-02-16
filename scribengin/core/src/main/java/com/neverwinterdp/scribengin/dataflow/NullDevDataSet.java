package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.nulldev.NullDevStorageConfig;

public class NullDevDataSet<T> extends DataSet<T> {
  private NullDevStorageConfig nullDevStorageConfig;
  
  public NullDevDataSet(DataStreamType type, NullDevStorageConfig nullDevStorageConfig) {
    super("nulldev", type);
    this.nullDevStorageConfig = nullDevStorageConfig;
  }
  
  @Override
  protected StorageConfig createStorageConfig() { return new NullDevStorageConfig(nullDevStorageConfig); }
}
