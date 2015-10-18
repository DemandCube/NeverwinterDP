package com.neverwinterdp.scribengin.storage.kafka.source;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;

import com.neverwinterdp.kafka.KafkaClient;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.source.Source;
import com.neverwinterdp.scribengin.storage.source.SourceStream;

public class KafkaSource implements Source {
  private KafkaClient kafkaClient;
  private StorageDescriptor descriptor;
  private Map<Integer, KafkaSourceStream> sourceStreams = new HashMap<Integer, KafkaSourceStream>();
  
  public KafkaSource(KafkaClient kafkaClient, String name, String topic) throws Exception {
    this.kafkaClient = kafkaClient;
    StorageDescriptor descriptor = createStorageDescriptor(name, topic, kafkaClient.getZkConnects(), null);
    init(descriptor);
  }
  
  public KafkaSource(KafkaClient kafkaClient, StorageDescriptor descriptor) throws Exception {
    this.kafkaClient = kafkaClient;
    init(descriptor);
  }

  void init(StorageDescriptor descriptor) throws Exception {
    this.descriptor = descriptor;
    TopicMetadata topicMetdadata = kafkaClient.findTopicMetadata(descriptor.attribute("topic"));
    List<PartitionMetadata> partitionMetadatas = topicMetdadata.partitionsMetadata();
    for(int i = 0; i < partitionMetadatas.size(); i++) {
      PartitionMetadata partitionMetadata = partitionMetadatas.get(i);
      KafkaSourceStream sourceStream = new KafkaSourceStream(kafkaClient, descriptor, partitionMetadata);
      sourceStreams.put(sourceStream.getId(), sourceStream);
    }
  }
  
  @Override
  public StorageDescriptor getDescriptor() { return descriptor; }

  /**
   * The stream id is equivalent to the partition id of the kafka
   */
  @Override
  public SourceStream getStream(int id) {  return sourceStreams.get(id); }

  @Override
  public SourceStream getStream(StreamDescriptor descriptor) {
    return sourceStreams.get(descriptor.getId());
  }

  @Override
  public SourceStream[] getStreams() {
    SourceStream[] array = new SourceStream[sourceStreams.size()];
    return sourceStreams.values().toArray(array);
  }
  
  public void close() throws Exception {
  }
  
  static public StorageDescriptor createStorageDescriptor(String name, String topic, String zkConnect, String reader) {
    StorageDescriptor descriptor = new StorageDescriptor("kafka");
    descriptor.attribute("name", name);
    descriptor.attribute("topic", topic);
    descriptor.attribute("zk.connect", zkConnect);
    if(reader != null) {
      descriptor.attribute("reader", reader);
    } else {
      descriptor.attribute("reader", "record");
    }
    return descriptor;
  }
}