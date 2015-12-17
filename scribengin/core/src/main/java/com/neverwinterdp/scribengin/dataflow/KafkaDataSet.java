package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.storage.kafka.KafkaStorageConfig;

public class KafkaDataSet<T> extends DataSet<T> {
  private KafkaStorageConfig kafkaStorageConfig;
  
  public KafkaDataSet(DataSetType type, KafkaStorageConfig kafkaStorageConfig) {
    super(kafkaStorageConfig.getName(), type);
    this.kafkaStorageConfig = kafkaStorageConfig;
  }
  
  public KafkaDataSet(String name, DataSetType type, String zkConnects, String topic) {
    super(name, type);
    kafkaStorageConfig = new KafkaStorageConfig();
    kafkaStorageConfig.setName(name);
    kafkaStorageConfig.setZKConnect(zkConnects);
    kafkaStorageConfig.setTopic(topic);
  }

  public KafkaDataSet<T> useRawReader() {
    this.kafkaStorageConfig.attribute("reader", "raw");
    return this;
  }
  
  @Override
  public KafkaStorageConfig getStorageConfig() { return kafkaStorageConfig; }
}
