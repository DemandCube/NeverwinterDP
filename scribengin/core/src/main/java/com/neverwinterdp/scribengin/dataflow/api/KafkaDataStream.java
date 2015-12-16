package com.neverwinterdp.scribengin.dataflow.api;

import com.neverwinterdp.storage.kafka.KafkaStorageConfig;

public class KafkaDataStream<T> extends DataStream<T> {
  private KafkaStorageConfig kafkaStorageConfig;
  
  public KafkaDataStream(DataStreamType type, KafkaStorageConfig kafkaStorageConfig) {
    super(kafkaStorageConfig.getName(), type);
    this.kafkaStorageConfig = kafkaStorageConfig;
  }
  
  public KafkaDataStream(String name, DataStreamType type, String zkConnects, String topic) {
    super(name, type);
    kafkaStorageConfig = new KafkaStorageConfig();
    kafkaStorageConfig.setName(name);
    kafkaStorageConfig.setZKConnect(zkConnects);
    kafkaStorageConfig.setTopic(topic);
  }

  @Override
  public KafkaStorageConfig getStorageConfig() { return kafkaStorageConfig; }
}
