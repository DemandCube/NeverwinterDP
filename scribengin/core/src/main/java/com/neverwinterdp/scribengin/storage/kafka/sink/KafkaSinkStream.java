package com.neverwinterdp.scribengin.storage.kafka.sink;

import com.neverwinterdp.kafka.KafkaClient;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.sink.SinkStream;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;

public class KafkaSinkStream implements SinkStream {
  private KafkaClient kafkaClient;
  private StreamDescriptor descriptor;
  
  public KafkaSinkStream(KafkaClient kafkaClient, StreamDescriptor descriptor) {
    this.kafkaClient = kafkaClient;
    this.descriptor  = descriptor;
  }
  
  @Override
  public StreamDescriptor getPartitionConfig() { return descriptor; }

  @Override
  public void delete() throws Exception {
  }

  @Override
  public SinkStreamWriter getWriter() throws Exception {
    return new KafkaSinkStreamWriter(kafkaClient, descriptor);
  }
  
  public void optimize() throws Exception {
  }
}
