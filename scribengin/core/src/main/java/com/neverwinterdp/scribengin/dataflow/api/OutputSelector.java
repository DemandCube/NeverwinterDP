package com.neverwinterdp.scribengin.dataflow.api;

public interface OutputSelector<IN> {
  public String select(IN in) ;
}
