package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.storage.StorageConfig;

abstract public class DataSet<T> {
  private String name;
  private DataSetType   type = DataSetType.Wire;
  
  public DataSet(String name, DataSetType type) {
    this.name = name;
    this.type = type ;
  }
  
  public String getName() { return this.name; }
  
  public <OUT> DataSet<T> connect(Operator<T, OUT> operator) {
    operator.in(this);
    return this;
  }
  
  abstract public StorageConfig getStorageConfig() ;
}
