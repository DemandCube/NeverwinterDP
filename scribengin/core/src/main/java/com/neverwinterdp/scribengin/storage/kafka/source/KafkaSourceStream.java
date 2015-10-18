package com.neverwinterdp.scribengin.storage.kafka.source;

import kafka.javaapi.PartitionMetadata;

import com.neverwinterdp.kafka.KafkaClient;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.source.SourceStream;
import com.neverwinterdp.scribengin.storage.source.SourceStreamReader;

public class KafkaSourceStream implements SourceStream {
  private KafkaClient      kafkaClient;
  private StreamDescriptor descriptor;
  private PartitionMetadata partitionMetadata;
  
  public KafkaSourceStream(KafkaClient kafkaClient, StorageDescriptor storageDescriptor, PartitionMetadata metadata) {
    this.kafkaClient = kafkaClient;
    descriptor = new StreamDescriptor(storageDescriptor);
    descriptor.setId(metadata.partitionId());
    this.partitionMetadata = metadata;
  }
  
  public int getId() { return descriptor.getId(); }
  
  @Override
  public StreamDescriptor getDescriptor() { return descriptor; }

  @Override
  public SourceStreamReader getReader(String name) throws Exception {
    String readerType = descriptor.attribute("reader") ;
    if("raw".equalsIgnoreCase(readerType)) {
      return new RawKafkaSourceStreamReader(kafkaClient, descriptor, partitionMetadata); 
    } else {
      return new KafkaSourceStreamReader(kafkaClient, descriptor, partitionMetadata);
    }
  }
}