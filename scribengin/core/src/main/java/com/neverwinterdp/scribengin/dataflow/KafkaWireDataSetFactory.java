package com.neverwinterdp.scribengin.dataflow;

public class KafkaWireDataSetFactory implements WireDataSetFactory {
  private String zkConnects ;
  
  public KafkaWireDataSetFactory(String zkConnects) {
    this.zkConnects = zkConnects;
  }
  
  @Override
  public <T> KafkaDataSet<T> createDataStream(Dataflow<?, ?> dfl, String name) {
    KafkaDataSet<T> ds = new KafkaDataSet<T>(name, DataSetType.Wire, zkConnects, dfl.getDataflowId() + "." + name);
    return ds;
  }
}
