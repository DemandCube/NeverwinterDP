package com.neverwinterdp.scribengin.dataflow.api;

import java.util.Map;

public class Operator<IN, OUT> {
  private String                       name;
  private Map<String, DataStream<IN>>  inStreams;
  private Map<String, DataStream<OUT>> outStreams;
  private OperatorFunction<IN, OUT>    operatorFunction;
  private OutputSelector<OUT>          outputSelector;

  public Operator(String name) {
    this.name = name;
  }
  
  public Operator(String name, OperatorFunction<IN, OUT> func, OutputSelector<OUT> selector) {
    this(name);
    operatorFunction = func;
    outputSelector   = selector;
  }

  public String getName() { return name; }
  public void setName(String name) { this.name = name; }
  
  public DataStream<IN> inConnect(DataStream<IN> in) {
    return in;
  }
  
  public DataStream<OUT> outConnect(DataStream<OUT> out) {
    return out;
  }
  
  public <T> Operator<OUT, T> outConnect(Operator<OUT, T> next) {
    return next;
  }
  
  public <T> Operator<OUT, T> outConnect(String name, OperatorFunction<OUT, T> func, OutputSelector<T> selector) {
    Operator<OUT, T> operator = new Operator<OUT, T>(name);
    return operator;
  }
}
