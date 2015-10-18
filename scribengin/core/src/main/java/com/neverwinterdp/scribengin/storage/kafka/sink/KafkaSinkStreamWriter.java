package com.neverwinterdp.scribengin.storage.kafka.sink;

import java.util.concurrent.atomic.AtomicInteger;

import com.neverwinterdp.kafka.KafkaClient;
import com.neverwinterdp.kafka.producer.AckKafkaWriter;
import com.neverwinterdp.kafka.producer.KafkaWriter;
import com.neverwinterdp.scribengin.dataflow.DataflowMessage;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;

//TODO: Allow the writer write to the assigned partition and configure the send time out
public class KafkaSinkStreamWriter implements SinkStreamWriter {
  static AtomicInteger idTracker = new AtomicInteger();
  
  private KafkaClient kafkaClient;
  private StreamDescriptor descriptor;
  private KafkaWriter writer ;
  private String topic;
  
  public KafkaSinkStreamWriter(KafkaClient kafkaClient, StreamDescriptor descriptor) {
    this.kafkaClient = kafkaClient;
    this.descriptor = descriptor;
    this.topic = descriptor.attribute("topic");
    String brokerUrls = descriptor.attribute("broker.list");
    writer = new AckKafkaWriter("topic." + writer, brokerUrls) ;
    //writer = new DefaultKafkaWriter("topic." + writer, brokerUrls) ;
  }
  
  @Override
  public void append(DataflowMessage dataflowMessage) throws Exception {
    writer.send(topic, dataflowMessage, 5000);
  }


  @Override
  public void close() throws Exception {
    writer.close();
  }

  @Override
  public void rollback() throws Exception {

  }

  @Override
  public void commit() throws Exception {
  }

  @Override
  public void prepareCommit() {
  }

  @Override
  public void completeCommit() {
  }
}
