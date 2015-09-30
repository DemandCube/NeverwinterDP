package com.neverwinterdp.scribengin.storage.kafka.sink;

import com.neverwinterdp.scribengin.storage.PartitionDescriptor;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStream;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStreamWriter;

public class KafkaSinkPartitionStream implements SinkPartitionStream {
  private PartitionDescriptor descriptor;
  
  public KafkaSinkPartitionStream(PartitionDescriptor descriptor) {
    this.descriptor = descriptor;
  }
  
  @Override
  public PartitionDescriptor getDescriptor() { return descriptor; }

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
