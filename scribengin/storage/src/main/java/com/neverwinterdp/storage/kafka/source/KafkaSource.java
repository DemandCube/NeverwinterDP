package com.neverwinterdp.storage.kafka.source;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.kafka.KafkaTool;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.source.Source;
import com.neverwinterdp.storage.source.SourcePartition;

public class KafkaSource implements Source {
  private KafkaTool            kafkaTool;
  private StorageConfig        storageConfig;
  private KafkaSourcePartition partition ;
  
  public KafkaSource(KafkaTool kafkaClient, String name, String topic) throws Exception {
    this(kafkaClient, createStorageConfig(name, topic, kafkaClient.getZkConnects(), null));
  }
  
  public KafkaSource(KafkaTool kafkaClient, StorageConfig sconfig) throws Exception {
    this.kafkaTool = kafkaClient;
    this.storageConfig = sconfig;
    this.partition = new KafkaSourcePartition(kafkaClient, storageConfig);
  }
  
  @Override
  public StorageConfig getStorageConfig() { return storageConfig; }

  @Override
  public SourcePartition getLatestSourcePartition() throws Exception { return partition; }

  @Override
  public List<SourcePartition> getSourcePartitions() throws Exception {
    List<SourcePartition> holder = new ArrayList<>();
    holder.add(partition);
    return holder;
  }
  
  static public StorageConfig createStorageConfig(String name, String topic, String zkConnect, String reader) {
    StorageConfig descriptor = new StorageConfig("kafka");
    descriptor.attribute("name", name);
    descriptor.attribute("topic", topic);
    descriptor.attribute("zk.connect", zkConnect);
    if(reader != null) {
      descriptor.attribute("reader", reader);
    } else {
      descriptor.attribute("reader", "record");
    }
    return descriptor;
  }
}
