package com.neverwinterdp.scribengin.dataflow.api;

public interface WireDataSetFactory {
  public <T> DataSet<T> createDataStream(Dataflow<?, ?> dataflow, String name) ;
}
