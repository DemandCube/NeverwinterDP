package com.neverwinterdp.scribengin.storage.kafka.source;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;

import com.neverwinterdp.kafka.tool.KafkaTool;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.PartitionDescriptor;
import com.neverwinterdp.scribengin.storage.source.Source;
import com.neverwinterdp.scribengin.storage.source.SourcePartitionStream;

public class KafkaSource implements Source {
  private StorageDescriptor descriptor;
  private Map<Integer, KafkaSourceStream> sourceStreams = new HashMap<Integer, KafkaSourceStream>();
  
  public KafkaSource(String name, String zkConnect, String topic) throws Exception {
    StorageDescriptor descriptor = createStorageDescriptor(name, topic, zkConnect, null);
    init(descriptor);
  }
  
  public KafkaSource(StorageDescriptor descriptor) throws Exception {
    init(descriptor);
  }

  void init(StorageDescriptor descriptor) throws Exception {
    this.descriptor = descriptor;
    KafkaTool kafkaTool = new KafkaTool(descriptor.attribute("name"), descriptor.attribute("zk.connect"));
    kafkaTool.connect(descriptor.attribute("zk.connect"));
    TopicMetadata topicMetdadata = kafkaTool.findTopicMetadata(descriptor.attribute("topic"));
    List<PartitionMetadata> partitionMetadatas = topicMetdadata.partitionsMetadata();
    for(int i = 0; i < partitionMetadatas.size(); i++) {
      PartitionMetadata partitionMetadata = partitionMetadatas.get(i);
      KafkaSourceStream sourceStream = new KafkaSourceStream(descriptor, partitionMetadata);
      sourceStreams.put(sourceStream.getId(), sourceStream);
    }
    kafkaTool.close();
  }
  
  @Override
  public StorageDescriptor getDescriptor() { return descriptor; }

  /**
   * The stream id is equivalent to the partition id of the kafka
   */
  @Override
  public SourcePartitionStream getStream(int id) {  return sourceStreams.get(id); }

  @Override
  public SourcePartitionStream getStream(PartitionDescriptor descriptor) {
    return sourceStreams.get(descriptor.getId());
  }

  @Override
  public SourcePartitionStream[] getStreams() {
    SourcePartitionStream[] array = new SourcePartitionStream[sourceStreams.size()];
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