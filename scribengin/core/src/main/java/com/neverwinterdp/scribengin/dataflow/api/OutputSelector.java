package com.neverwinterdp.scribengin.dataflow.api;

public interface OutputSelector<OUT> {
  public String select(OUT out, String[] dsNames) ;
}
