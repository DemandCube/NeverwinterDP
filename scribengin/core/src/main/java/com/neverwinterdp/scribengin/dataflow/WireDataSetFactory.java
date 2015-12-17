package com.neverwinterdp.scribengin.dataflow;

public interface WireDataSetFactory {
  public <T> DataSet<T> createDataStream(Dataflow<?, ?> dataflow, String name) ;
}
