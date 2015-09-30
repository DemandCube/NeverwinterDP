package com.neverwinterdp.scribengin.storage.kafka;

import com.neverwinterdp.kafka.tool.KafkaTool;
import com.neverwinterdp.scribengin.storage.Storage;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.kafka.sink.KafkaSink;
import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.source.Source;

public class KafkaStorage extends Storage {
  final static public String NAME       = "name";
  final static public String TOPIC      = "topic";
  final static public String ZK_CONNECT = "zk.connect";
  
  public KafkaStorage(StorageDescriptor storageDescriptor) {
    super(storageDescriptor);
  }
  
  public KafkaStorage(String name, String zkConnect, String topic) throws Exception {
    super(createDescriptor(name, zkConnect, topic));
  }
  

  @Override
  public void refresh() throws Exception {
  }

  public boolean exists() throws Exception {
    StorageDescriptor descriptor = getStorageDescriptor();
    KafkaTool kafkaTool = getKafkaTool(descriptor) ;
    boolean exists = kafkaTool.topicExits(descriptor.attribute(TOPIC));
    kafkaTool.close();
    return exists;
  }
  
  @Override
  public void drop() throws Exception {
    StorageDescriptor descriptor = getStorageDescriptor();
    KafkaTool kafkaTool = getKafkaTool(getStorageDescriptor()) ;
    kafkaTool.deleteTopic(descriptor.attribute(TOPIC));
    kafkaTool.close();
  }

  @Override
  public void create(int numOfPartition, int replication) throws Exception {
    StorageDescriptor descriptor = getStorageDescriptor();
    KafkaTool kafkaTool = getKafkaTool(descriptor) ;
    kafkaTool.createTopic(descriptor.attribute(TOPIC), numOfPartition, replication);
    kafkaTool.close();
  }

  @Override
  public Sink getSink() throws Exception {
    return new KafkaSink(getStorageDescriptor());
  }

  @Override
  public Source getSource() throws Exception {
    return null;
  }

  static StorageDescriptor createDescriptor(String name, String zkConnect, String topic) {
    StorageDescriptor descriptor = new StorageDescriptor("kafka");
    descriptor.attribute(NAME, name);
    descriptor.attribute(TOPIC, topic);
    descriptor.attribute(ZK_CONNECT, zkConnect);
    return descriptor;
  }
  
  static public KafkaTool getKafkaTool(StorageDescriptor descriptor) throws Exception {
    String name = descriptor.attribute(KafkaStorage.NAME);
    String zkConnect = descriptor.attribute(KafkaStorage.ZK_CONNECT);
    KafkaTool kafkaTool = new KafkaTool(name, zkConnect) ;
    kafkaTool.connect();
    return kafkaTool;
  }
}
