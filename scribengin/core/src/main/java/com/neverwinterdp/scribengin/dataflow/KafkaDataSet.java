package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.kafka.KafkaStorageConfig;

public class KafkaDataSet<T> extends DataSet<T> {
  private KafkaStorageConfig kafkaStorageConfig;
  
  public KafkaDataSet(DataStreamType type, KafkaStorageConfig kafkaStorageConfig) {
    super(kafkaStorageConfig.getName(), type);
    this.kafkaStorageConfig = kafkaStorageConfig;
  }
  
  public KafkaDataSet(String name, DataStreamType type, String zkConnects, String topic) {
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
  protected StorageConfig createStorageConfig() { 
    return new KafkaStorageConfig(kafkaStorageConfig); 
  }
}
