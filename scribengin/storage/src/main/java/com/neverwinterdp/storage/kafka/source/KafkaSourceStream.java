package com.neverwinterdp.storage.kafka.source;

import kafka.javaapi.PartitionMetadata;

import com.neverwinterdp.kafka.KafkaTool;
import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.source.SourcePartitionStream;
import com.neverwinterdp.storage.source.SourcePartitionStreamReader;

public class KafkaSourceStream implements SourcePartitionStream {
  private KafkaTool         kafkaTool;
  private StorageConfig     storageConfig;
  private PartitionStreamConfig   partitionConfig;
  private PartitionMetadata partitionMetadata;

  public KafkaSourceStream(KafkaTool kafkaClient, StorageConfig storageConfig, PartitionMetadata pmd) {
    this.kafkaTool = kafkaClient;
    partitionConfig = new PartitionStreamConfig(storageConfig);
    partitionConfig.setPartitionStreamId(pmd.partitionId());
    partitionMetadata = pmd;
  }
  
  public int getId() { return partitionConfig.getPartitionStreamId(); }
  
  @Override
  public PartitionStreamConfig getPartitionStreamConfig() { return partitionConfig; }

  @Override
  public SourcePartitionStreamReader getReader(String readerName) throws Exception {
    String readerType = partitionConfig.attribute("reader") ;
    if("raw".equalsIgnoreCase(readerType)) {
      return new RawKafkaSourceStreamReader(readerName, kafkaTool, partitionConfig, partitionMetadata); 
    } else {
      return new KafkaSourceStreamReader(readerName, kafkaTool, partitionConfig, partitionMetadata);
    }
  }
}