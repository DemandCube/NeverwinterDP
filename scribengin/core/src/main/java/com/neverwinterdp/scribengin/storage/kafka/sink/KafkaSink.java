package com.neverwinterdp.scribengin.storage.kafka.sink;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import com.neverwinterdp.kafka.KafkaClient;
import com.neverwinterdp.scribengin.storage.PartitionStreamConfig;
import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.kafka.KafkaStorage;
import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStream;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;

public class KafkaSink implements Sink {
  private StorageConfig storageConfig;
  private KafkaClient   kafkaClient;
  
  public KafkaSink(KafkaClient kafkaClient, String name, String topic) throws Exception {
    this.kafkaClient = kafkaClient;
    init(KafkaStorage.createStorageConfig(name, kafkaClient.getZkConnects(), topic)) ;
  }
  
  public KafkaSink(KafkaClient kafkaClient, StorageConfig descriptor) throws Exception {
    this.kafkaClient = kafkaClient;
    init(descriptor) ;
  }
  
  private void init(StorageConfig descriptor) throws Exception {
    descriptor.attribute("broker.list", kafkaClient.getKafkaBrokerList());
    this.storageConfig  = descriptor ;
  }
  
  @Override
  public StorageConfig getDescriptor() { return storageConfig; }

  public List<PartitionStreamConfig> getPartitionStreamConfigs() throws Exception {
    TopicMetadata tMetadata = kafkaClient.findTopicMetadata(storageConfig.attribute(KafkaStorage.TOPIC));
    List<PartitionStreamConfig> pConfigs = new ArrayList<>();
    List<PartitionMetadata> partitions = tMetadata.partitionsMetadata();
    for(int i = 0; i < partitions.size(); i++) {
      PartitionMetadata pmetadata = partitions.get(i);
      PartitionStreamConfig pConfig = new PartitionStreamConfig(pmetadata.partitionId());
      pConfigs.add(pConfig);
    }
    return pConfigs ;
  }
  
  @Override
  public SinkPartitionStream getPartitionStream(PartitionStreamConfig pConfig) throws Exception {
    KafkaSinkPartitionStream newStream= new KafkaSinkPartitionStream(storageConfig, pConfig) ;
    return newStream;
  }

  @Override
  public SinkPartitionStream getParitionStream(int partitionId) throws Exception {
    PartitionStreamConfig pConfig = new PartitionStreamConfig(partitionId);
    KafkaSinkPartitionStream newStream= new KafkaSinkPartitionStream(storageConfig, pConfig) ;
    return newStream;
  }

  
  @Override
  public SinkPartitionStream[] getPartitionStreams() throws Exception {
    List<PartitionStreamConfig> pConfigs = getPartitionStreamConfigs();
    SinkPartitionStream[] streams = new SinkPartitionStream[pConfigs.size()];
    for(int i = 0; i < pConfigs.size(); i++) {
      streams[i] = new KafkaSinkPartitionStream(storageConfig, pConfigs.get(i)) ;
    }
    return streams;
  }

  @Override
  public void close() throws Exception {
  }
}
