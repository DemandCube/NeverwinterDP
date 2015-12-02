package com.neverwinterdp.storage.kafka.source;

import kafka.javaapi.PartitionMetadata;

import com.neverwinterdp.kafka.KafkaClient;
import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.source.SourcePartitionStream;
import com.neverwinterdp.storage.source.SourcePartitionStreamReader;

public class KafkaSourceStream implements SourcePartitionStream {
  private KafkaClient       kafkaClient;
  private StorageConfig     storageConfig;
  private PartitionStreamConfig   partitionConfig;
  private PartitionMetadata partitionMetadata;

  public KafkaSourceStream(KafkaClient kafkaClient, StorageConfig storageConfig, PartitionMetadata pmd) {
    this.kafkaClient = kafkaClient;
    partitionConfig = new PartitionStreamConfig(storageConfig);
    partitionConfig.setPartitionStreamId(pmd.partitionId());
    this.partitionMetadata = pmd;
  }
  
  public int getId() { return partitionConfig.getPartitionStreamId(); }
  
  @Override
  public PartitionStreamConfig getPartitionStreamConfig() { return partitionConfig; }

  @Override
  public SourcePartitionStreamReader getReader(String name) throws Exception {
    String readerType = partitionConfig.attribute("reader") ;
    if("raw".equalsIgnoreCase(readerType)) {
      return new RawKafkaSourceStreamReader(kafkaClient, partitionConfig, partitionMetadata); 
    } else {
      return new KafkaSourceStreamReader(kafkaClient, partitionConfig, partitionMetadata);
    }
  }
}