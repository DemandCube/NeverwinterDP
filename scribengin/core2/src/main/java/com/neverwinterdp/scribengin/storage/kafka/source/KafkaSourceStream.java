package com.neverwinterdp.scribengin.storage.kafka.source;

import kafka.javaapi.PartitionMetadata;

import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.PartitionConfig;
import com.neverwinterdp.scribengin.storage.source.SourcePartitionStream;
import com.neverwinterdp.scribengin.storage.source.SourcePartitionStreamReader;

public class KafkaSourceStream implements SourcePartitionStream {
  private StorageConfig storageConfig;
  private PartitionConfig partitionConfig;
  private PartitionMetadata partitionMetadata;
  
  public KafkaSourceStream(StorageConfig storageConfig, PartitionMetadata pmd) {
    partitionConfig = new PartitionConfig(storageConfig);
    partitionConfig.setPartitionId(pmd.partitionId());
    this.partitionMetadata = pmd;
  }
  
  public int getId() { return partitionConfig.getPartitionId(); }
  
  @Override
  public PartitionConfig getDescriptor() { return partitionConfig; }

  @Override
  public SourcePartitionStreamReader getReader(String name) throws Exception {
    String readerType = partitionConfig.attribute("reader") ;
    if("raw".equalsIgnoreCase(readerType)) {
      return new RawKafkaSourceStreamReader(partitionConfig, partitionMetadata); 
    } else {
      return new KafkaSourceStreamReader(partitionConfig, partitionMetadata);
    }
  }
}