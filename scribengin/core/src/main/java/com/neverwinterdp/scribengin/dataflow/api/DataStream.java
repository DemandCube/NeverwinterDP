package com.neverwinterdp.scribengin.dataflow.api;

import com.neverwinterdp.storage.StorageConfig;

abstract public class DataStream<T> {
  private String name;
  private DataStreamType   type = DataStreamType.Wire;
  
  public DataStream(String name, DataStreamType type) {
    this.name = name;
    this.type = type ;
  }
  
  public String getName() { return this.name; }
  
  public <OUT> DataStream<T> connect(Operator<T, OUT> operator) {
    operator.in(this);
    return this;
  }
  
  abstract public StorageConfig getStorageConfig() ;
}
