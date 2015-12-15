package com.neverwinterdp.scribengin.dataflow.api;

public class KafkaDataStream<T> extends DataStream<T> {
  private String zkConnects;
  
  public KafkaDataStream(String name, String zkConnects) {
    super(name);
    this.zkConnects = zkConnects;
  }

  public String getZkConnects() { return zkConnects; }
  public void setZkConnects(String zkConnects) {
    this.zkConnects = zkConnects;
  }
}
