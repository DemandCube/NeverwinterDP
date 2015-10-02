package com.neverwinterdp.scribengin.storage.kafka;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.kafka.consumer.KafkaPartitionReader;
import com.neverwinterdp.kafka.tool.KafkaTool;
import com.neverwinterdp.scribengin.storage.PartitionConfig;
import com.neverwinterdp.scribengin.storage.Storage;
import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.kafka.sink.KafkaSink;
import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.source.Source;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;

public class KafkaStorage extends Storage {
  final static public String NAME       = "name";
  final static public String TOPIC      = "topic";
  final static public String ZK_CONNECT = "zk.connect";
  
  public KafkaStorage(StorageConfig storageDescriptor) {
    super(storageDescriptor);
  }
  
  public KafkaStorage(String name, String zkConnect, String topic) throws Exception {
    super(createStorageConfig(name, zkConnect, topic));
  }
  
  public List<PartitionConfig> getPartitionConfigs() throws Exception {
    StorageConfig sConfig = getStorageConfig();
    KafkaTool kafkaTool = getKafkaTool(sConfig) ;
    TopicMetadata tMetadata = kafkaTool.findTopicMetadata(sConfig.attribute(TOPIC));
    kafkaTool.close();
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
  }

  public boolean exists() throws Exception {
    StorageConfig sConfig = getStorageConfig();
    KafkaTool kafkaTool = getKafkaTool(sConfig) ;
    boolean exists = kafkaTool.topicExits(sConfig.attribute(TOPIC));
    kafkaTool.close();
    return exists;
  }
  
  @Override
  public void drop() throws Exception {
    StorageConfig descriptor = getStorageConfig();
    KafkaTool kafkaTool = getKafkaTool(getStorageConfig()) ;
    kafkaTool.deleteTopic(descriptor.attribute(TOPIC));
    kafkaTool.close();
  }

  @Override
  public void create(int numOfPartition, int replication) throws Exception {
    StorageConfig descriptor = getStorageConfig();
    KafkaTool kafkaTool = getKafkaTool(descriptor) ;
    kafkaTool.createTopic(descriptor.attribute(TOPIC), replication, numOfPartition);
    kafkaTool.close();
  }

  @Override
  public Sink getSink() throws Exception {
    return new KafkaSink(getStorageConfig());
  }

  @Override
  public Source getSource() throws Exception {
    return null;
  }

  static StorageConfig createStorageConfig(String name, String zkConnect, String topic) {
    StorageConfig descriptor = new StorageConfig("kafka");
    descriptor.attribute(NAME, name);
    descriptor.attribute(TOPIC, topic);
    descriptor.attribute(ZK_CONNECT, zkConnect);
    return descriptor;
  }
  
  static public KafkaTool getKafkaTool(StorageConfig descriptor) throws Exception {
    String name = descriptor.attribute(KafkaStorage.NAME);
    String zkConnect = descriptor.attribute(KafkaStorage.ZK_CONNECT);
    KafkaTool kafkaTool = new KafkaTool(name, zkConnect) ;
    kafkaTool.connect();
    return kafkaTool;
  }
  
  static public KafkaPartitionReader getKafkaPartitionReader(StorageConfig sconfig, PartitionMetadata metadata) throws Exception {
    String name = sconfig.attribute(NAME);
    String zkConnect = sconfig.attribute(ZK_CONNECT);
    String topic = sconfig.attribute(TOPIC);
    KafkaPartitionReader partitionReader = new KafkaPartitionReader(name, zkConnect, topic, metadata);
    return partitionReader;
  }
}
