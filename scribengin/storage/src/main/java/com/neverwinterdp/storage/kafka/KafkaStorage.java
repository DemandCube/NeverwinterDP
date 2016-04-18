package com.neverwinterdp.storage.kafka;

import com.neverwinterdp.kafka.KafkaTool;
import com.neverwinterdp.storage.Storage;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.kafka.sink.KafkaSink;
import com.neverwinterdp.storage.kafka.source.KafkaSource;
import com.neverwinterdp.storage.sink.Sink;
import com.neverwinterdp.storage.source.Source;

public class KafkaStorage extends Storage {
  private KafkaTool kafkaTool ;
  private KafkaSource kafkaSource ;
  
  public KafkaStorage(KafkaTool kafkaClient, StorageConfig storageDescriptor) {
    super(storageDescriptor);
    this.kafkaTool = kafkaClient;
  }
  
  public KafkaStorage(KafkaTool kafkaClient, String name, String topic) throws Exception {
    super(new  KafkaStorageConfig(name, kafkaClient.getZkConnects(), topic));
    this.kafkaTool = kafkaClient;
  }
  
  @Override
  public void refresh() throws Exception {
    kafkaSource  = null ;
  }

  public boolean exists() throws Exception {
    String topic = getStorageConfig().attribute(KafkaStorageConfig.TOPIC);
    boolean exists = kafkaTool.getKafkaTool().topicExits(topic);
    return exists;
  }
  
  @Override
  public void drop() throws Exception {
    String topic = getStorageConfig().attribute(KafkaStorageConfig.TOPIC);
    kafkaTool.getKafkaTool().deleteTopic(topic);
    kafkaSource  = null ;
  }

  @Override
  public void create() throws Exception {
    StorageConfig config = getStorageConfig();
    String topic = config.attribute(KafkaStorageConfig.TOPIC);
    kafkaTool.getKafkaTool().createTopic(topic, config.getReplication(), config.getPartitionStream());
    kafkaSource = null;
  }

  @Override
  public Sink getSink() throws Exception {
    return new KafkaSink(kafkaTool, getStorageConfig());
  }

  @Override
  public Source getSource() throws Exception {
    if(kafkaSource == null) {
      kafkaSource = new KafkaSource(kafkaTool, getStorageConfig());
    }
    return kafkaSource;
  }
}
