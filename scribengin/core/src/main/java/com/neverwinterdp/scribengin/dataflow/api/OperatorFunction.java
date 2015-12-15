package com.neverwinterdp.scribengin.dataflow.api;

public interface OperatorFunction<IN, OUT> {
  public OUT apply(IN in) ;
  public String select(String[] availableDs);
}
