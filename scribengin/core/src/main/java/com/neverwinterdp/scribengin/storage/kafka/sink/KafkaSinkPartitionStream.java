package com.neverwinterdp.scribengin.storage.kafka.sink;

import com.neverwinterdp.scribengin.storage.PartitionStreamConfig;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStream;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStreamWriter;

public class KafkaSinkPartitionStream implements SinkPartitionStream {
  private PartitionStreamConfig partitionStreamConfig;
  
  public KafkaSinkPartitionStream(PartitionStreamConfig descriptor) {
    this.partitionStreamConfig = descriptor;
  }
 
  @Override
  public int getPartitionStreamId() { return partitionStreamConfig.getPartitionStreamId(); }
  
  @Override
  public void delete() throws Exception {
  }

  @Override
  public SinkPartitionStreamWriter getWriter() throws Exception {
    return new KafkaSinkPartitionStreamWriter(partitionStreamConfig);
  }
  
  public void optimize() throws Exception {
  }
 
}
