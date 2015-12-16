package com.neverwinterdp.scribengin.dataflow.api;

public interface WireDataStreamFactory {
  public <T> DataStream<T> createDataStream(Dataflow<?, ?> dataflow, String name) ;
}
