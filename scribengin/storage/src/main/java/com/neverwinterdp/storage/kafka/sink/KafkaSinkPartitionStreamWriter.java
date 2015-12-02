package com.neverwinterdp.storage.kafka.sink;

import com.neverwinterdp.kafka.producer.AckKafkaWriter;
import com.neverwinterdp.kafka.producer.KafkaWriter;
import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.Record;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.sink.SinkPartitionStreamWriter;

//TODO: Allow the writer write to the assigned partition and configure the send time out
public class KafkaSinkPartitionStreamWriter implements SinkPartitionStreamWriter {
  private StorageConfig         storageConfig;
  private PartitionStreamConfig partitionStreamConfig;
  private KafkaWriter writer ;
  private String topic;
  
  public KafkaSinkPartitionStreamWriter(StorageConfig storageConfig, PartitionStreamConfig partitionStreamConfig) {
    this.partitionStreamConfig = partitionStreamConfig;
    this.writer = new AckKafkaWriter(storageConfig.attribute("name"), storageConfig.attribute("broker.list")) ;
    this.topic = storageConfig.attribute("topic");
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
    prepareCommit();
    completeCommit();
  }

  @Override
  public void prepareCommit() throws Exception {
    writer.commit();
  }

  @Override
  public void completeCommit() {
  }
}
