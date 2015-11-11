package com.neverwinterdp.scribengin.storage.kafka.sink;

import com.neverwinterdp.scribengin.storage.PartitionStreamConfig;
import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStream;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStreamWriter;

public class KafkaSinkPartitionStream implements SinkPartitionStream {
  private StorageConfig         storageConfig;
  private PartitionStreamConfig partitionStreamConfig;
  
  public KafkaSinkPartitionStream(StorageConfig storageConfig, PartitionStreamConfig descriptor) {
    this.storageConfig = storageConfig;
    this.partitionStreamConfig = descriptor;
  }
 
  @Override
  public int getPartitionStreamId() { return partitionStreamConfig.getPartitionStreamId(); }
  
  @Override
  public void delete() throws Exception {
  }

  @Override
  public SinkPartitionStreamWriter getWriter() throws Exception {
    return new KafkaSinkPartitionStreamWriter(storageConfig, partitionStreamConfig);
  }
  
  public void optimize() throws Exception {
  }
 
}
