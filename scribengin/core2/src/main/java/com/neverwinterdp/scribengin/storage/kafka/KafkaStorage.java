package com.neverwinterdp.scribengin.storage.kafka;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.kafka.KafkaClient;
import com.neverwinterdp.kafka.consumer.KafkaPartitionReader;
import com.neverwinterdp.scribengin.storage.PartitionConfig;
import com.neverwinterdp.scribengin.storage.Storage;
import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.kafka.sink.KafkaSink;
import com.neverwinterdp.scribengin.storage.kafka.source.KafkaSource;
import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.source.Source;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;

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
  
  public List<PartitionConfig> getPartitionConfigs() throws Exception {
    StorageConfig sConfig = getStorageConfig();
    TopicMetadata tMetadata = kafkaClient.findTopicMetadata(sConfig.attribute(TOPIC));
    List<PartitionConfig> pConfigs = new ArrayList<>();
    List<PartitionMetadata> partitions = tMetadata.partitionsMetadata();
    for(int i = 0; i < partitions.size(); i++) {
      PartitionMetadata pmetadata = partitions.get(i);
      PartitionConfig pConfig = new PartitionConfig(pmetadata.partitionId());
      pConfigs.add(pConfig);
    }
    return pConfigs ;
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
