package com.neverwinterdp.scribengin.storage.kafka.source;

import kafka.javaapi.PartitionMetadata;

import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.PartitionDescriptor;
import com.neverwinterdp.scribengin.storage.source.SourcePartitionStream;
import com.neverwinterdp.scribengin.storage.source.SourcePartitionStreamReader;

public class KafkaSourceStream implements SourcePartitionStream {
  private PartitionDescriptor descriptor;
  private PartitionMetadata partitionMetadata;
  
  public KafkaSourceStream(StorageDescriptor storageDescriptor, PartitionMetadata metadata) {
    descriptor = new PartitionDescriptor(storageDescriptor);
    descriptor.setId(metadata.partitionId());
    this.partitionMetadata = metadata;
  }
  
  public int getId() { return descriptor.getId(); }
  
  @Override
  public PartitionDescriptor getDescriptor() { return descriptor; }

  @Override
  public SourcePartitionStreamReader getReader(String name) throws Exception {
    String readerType = descriptor.attribute("reader") ;
    if("raw".equalsIgnoreCase(readerType)) {
      return new RawKafkaSourceStreamReader(descriptor, partitionMetadata); 
    } else {
      return new KafkaSourceStreamReader(descriptor, partitionMetadata);
    }
  }
}