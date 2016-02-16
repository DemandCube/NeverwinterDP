package com.neverwinterdp.storage.kafka;

import com.neverwinterdp.kafka.KafkaClient;
import com.neverwinterdp.storage.Storage;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.kafka.sink.KafkaSink;
import com.neverwinterdp.storage.kafka.source.KafkaSource;
import com.neverwinterdp.storage.sink.Sink;
import com.neverwinterdp.storage.source.Source;

public class KafkaStorage extends Storage {
  private KafkaClient kafkaClient ;
  private KafkaSource kafkaSource ;
  
  public KafkaStorage(KafkaClient kafkaClient, StorageConfig storageDescriptor) {
    super(storageDescriptor);
    this.kafkaClient = kafkaClient;
  }
  
  public KafkaStorage(KafkaClient kafkaClient, String name, String topic) throws Exception {
    super(new  KafkaStorageConfig(name, kafkaClient.getZkConnects(), topic));
    this.kafkaClient = kafkaClient;
  }
  
  @Override
  public void refresh() throws Exception {
    kafkaSource  = null ;
  }

  public boolean exists() throws Exception {
    String topic = getStorageConfig().attribute(KafkaStorageConfig.TOPIC);
    boolean exists = kafkaClient.getKafkaTool().topicExits(topic);
    return exists;
  }
  
  @Override
  public void drop() throws Exception {
    String topic = getStorageConfig().attribute(KafkaStorageConfig.TOPIC);
    kafkaClient.getKafkaTool().deleteTopic(topic);
    kafkaSource  = null ;
  }

  @Override
  public void create() throws Exception {
    StorageConfig config = getStorageConfig();
    String topic = config.attribute(KafkaStorageConfig.TOPIC);
    kafkaClient.getKafkaTool().createTopic(topic, config.getReplication(), config.getPartitionStream());
    kafkaSource = null;
  }

  @Override
  public Sink getSink() throws Exception {
    return new KafkaSink(kafkaClient, getStorageConfig());
  }

  @Override
  public Source getSource() throws Exception {
    if(kafkaSource == null) {
      kafkaSource = new KafkaSource(kafkaClient, getStorageConfig());
    }
    return kafkaSource;
  }
}
