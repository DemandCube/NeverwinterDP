package com.neverwinterdp.scribengin.storage.kafka.sink;

import com.neverwinterdp.kafka.producer.AckKafkaWriter;
import com.neverwinterdp.kafka.producer.KafkaWriter;
import com.neverwinterdp.scribengin.storage.PartitionConfig;
import com.neverwinterdp.scribengin.storage.Record;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStreamWriter;

//TODO: Allow the writer write to the assigned partition and configure the send time out
public class KafkaSinkPartitionStreamWriter implements SinkPartitionStreamWriter {
  private PartitionConfig descriptor;
  private KafkaWriter writer ;
  private String topic;
  
  public KafkaSinkPartitionStreamWriter(PartitionConfig descriptor) {
    this.descriptor = descriptor;
    this.writer = new AckKafkaWriter(descriptor.attribute("name"), descriptor.attribute("broker.list")) ;
    this.topic = descriptor.attribute("topic");
  }
  
  @Override
  public void append(Record record) throws Exception {
    writer.send(topic, record, 5000);
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
