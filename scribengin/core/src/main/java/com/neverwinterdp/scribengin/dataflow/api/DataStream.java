package com.neverwinterdp.scribengin.dataflow.api;

public class DataStream<T> {
  private String name;
  
  public DataStream(String name) {
    this.name = name;
  }
  
  public String getName() { return this.name; }
  
  public <OUT> Operator<T, OUT> connect(Operator<T, OUT> operator) {
    return operator;
  }
}
