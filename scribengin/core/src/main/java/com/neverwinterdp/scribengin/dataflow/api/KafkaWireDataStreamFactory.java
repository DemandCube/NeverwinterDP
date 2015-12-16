package com.neverwinterdp.scribengin.dataflow.api;

public class KafkaWireDataStreamFactory implements WireDataStreamFactory {
  private String zkConnects ;
  
  public KafkaWireDataStreamFactory(String zkConnects) {
    this.zkConnects = zkConnects;
  }
  
  @Override
  public <T> KafkaDataStream<T> createDataStream(Dataflow<?, ?> dfl, String name) {
    KafkaDataStream<T> ds = new KafkaDataStream<T>(name, DataStreamType.Wire, zkConnects, dfl.getDataflowId() + "." + name);
    return ds;
  }
}
