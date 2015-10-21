package com.neverwinterdp.scribengin.storage.kafka.sink;

import com.neverwinterdp.scribengin.storage.PartitionConfig;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStream;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStreamWriter;

public class KafkaSinkPartitionStream implements SinkPartitionStream {
  private PartitionConfig descriptor;
  
  public KafkaSinkPartitionStream(PartitionConfig descriptor) {
    this.descriptor = descriptor;
  }
  
  @Override
  public PartitionConfig getParitionConfig() { return descriptor; }

  @Override
  public void delete() throws Exception {
  }

  @Override
  public SinkPartitionStreamWriter getWriter() throws Exception {
    return new KafkaSinkPartitionStreamWriter(descriptor);
  }
  
  public void optimize() throws Exception {
  }
}
