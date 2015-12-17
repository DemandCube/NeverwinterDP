package com.neverwinterdp.scribengin.dataflow;

public interface OperatorFunction<IN, OUT> {
  public OUT apply(IN in) ;
}
