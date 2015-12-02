package com.neverwinterdp.storage.kafka;

import com.neverwinterdp.kafka.KafkaClient;
import com.neverwinterdp.kafka.consumer.KafkaPartitionReader;
import com.neverwinterdp.storage.Storage;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.kafka.sink.KafkaSink;
import com.neverwinterdp.storage.kafka.source.KafkaSource;
import com.neverwinterdp.storage.sink.Sink;
import com.neverwinterdp.storage.source.Source;

import kafka.javaapi.PartitionMetadata;

public class KafkaStorage extends Storage {
  final static public String NAME       = "name";
  final static public String TOPIC      = "topic";
  final static public String ZK_CONNECT = "zk.connect";
  
  private KafkaClient kafkaClient ;
  private KafkaSource kafkaSource ;
  
  public KafkaStorage(KafkaClient kafkaClient, StorageConfig storageDescriptor) {
    super(storageDescriptor);
    this.kafkaClient = kafkaClient;
  }
  
  public KafkaStorage(KafkaClient kafkaClient, String name, String topic) throws Exception {
    super(createStorageConfig(name, kafkaClient.getZkConnects(), topic));
    this.kafkaClient = kafkaClient;
  }
  
  @Override
  public void refresh() throws Exception {
    kafkaSource  = null ;
  }

  public boolean exists() throws Exception {
    StorageConfig sConfig = getStorageConfig();
    boolean exists = kafkaClient.getKafkaTool().topicExits(sConfig.attribute(TOPIC));
    return exists;
  }
  
  @Override
  public void drop() throws Exception {
    StorageConfig descriptor = getStorageConfig();
    kafkaClient.getKafkaTool().deleteTopic(descriptor.attribute(TOPIC));
    kafkaSource  = null ;
  }

  @Override
  public void create(int numOfPartition, int replication) throws Exception {
    StorageConfig descriptor = getStorageConfig();
    kafkaClient.getKafkaTool().createTopic(descriptor.attribute(TOPIC), replication, numOfPartition);
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

  static public StorageConfig createStorageConfig(String name, String zkConnect, String topic) {
    StorageConfig descriptor = new StorageConfig("kafka");
    descriptor.attribute(NAME, name);
    descriptor.attribute(TOPIC, topic);
    descriptor.attribute(ZK_CONNECT, zkConnect);
    return descriptor;
  }
  
  
  static public KafkaPartitionReader getKafkaPartitionReader(KafkaClient kafkaClient, StorageConfig sconfig, PartitionMetadata metadata) throws Exception {
    String name = sconfig.attribute(NAME);
    String topic = sconfig.attribute(TOPIC);
    KafkaPartitionReader partitionReader = new KafkaPartitionReader(name, kafkaClient, topic, metadata);
    return partitionReader;
  }
}
