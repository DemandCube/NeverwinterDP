package com.neverwinterdp.scribengin.dataflow;

public interface OutputSelector<OUT> {
  public String select(OUT out, String[] dsNames) ;
}
