  package com.neverwinterdp.storage.kafka.sink;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.kafka.KafkaTool;
import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.kafka.KafkaStorageConfig;
import com.neverwinterdp.storage.sink.Sink;
import com.neverwinterdp.storage.sink.SinkPartitionStream;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;

public class KafkaSink implements Sink {
  private StorageConfig storageConfig;
  private KafkaTool     kafkaTool;
  
  public KafkaSink(KafkaTool kafkaClient, String name, String topic) throws Exception {
    this.kafkaTool = kafkaClient;
    init(new  KafkaStorageConfig(name, kafkaClient.getZkConnects(), topic)) ;
  }
  
  public KafkaSink(KafkaTool kafkaClient, StorageConfig descriptor) throws Exception {
    this.kafkaTool = kafkaClient;
    init(descriptor) ;
  }
  
  private void init(StorageConfig descriptor) throws Exception {
    descriptor.attribute("broker.list", kafkaTool.getKafkaBrokerList());
    this.storageConfig  = descriptor ;
  }
  
  @Override
  public StorageConfig getStorageConfig() { return storageConfig; }

  public List<PartitionStreamConfig> getPartitionStreamConfigs() throws Exception {
    String topic = storageConfig.attribute(KafkaStorageConfig.TOPIC);
    TopicMetadata tMetadata = kafkaTool.findTopicMetadata(topic);
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
  public SinkPartitionStream getPartitionStream(int partitionId) throws Exception {
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
