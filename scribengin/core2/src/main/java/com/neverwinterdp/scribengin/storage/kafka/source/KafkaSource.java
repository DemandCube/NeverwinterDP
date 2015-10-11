package com.neverwinterdp.scribengin.storage.kafka.source;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;

import com.neverwinterdp.kafka.tool.KafkaTool;
import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.PartitionConfig;
import com.neverwinterdp.scribengin.storage.source.Source;
import com.neverwinterdp.scribengin.storage.source.SourcePartitionStream;

public class KafkaSource implements Source {
  private StorageConfig descriptor;
  private Map<Integer, KafkaSourceStream> sourceStreams = new HashMap<Integer, KafkaSourceStream>();
  
  public KafkaSource(String name, String zkConnect, String topic) throws Exception {
    StorageConfig descriptor = createStorageDescriptor(name, topic, zkConnect, null);
    init(descriptor);
  }
  
  public KafkaSource(StorageConfig descriptor) throws Exception {
    init(descriptor);
  }

  void init(StorageConfig descriptor) throws Exception {
    this.descriptor = descriptor;
    KafkaTool kafkaTool = new KafkaTool(descriptor.attribute("name"), descriptor.attribute("zk.connect"));
    TopicMetadata topicMetdadata = kafkaTool.findTopicMetadata(descriptor.attribute("topic"));
    List<PartitionMetadata> partitionMetadatas = topicMetdadata.partitionsMetadata();
    for(int i = 0; i < partitionMetadatas.size(); i++) {
      PartitionMetadata partitionMetadata = partitionMetadatas.get(i);
      KafkaSourceStream sourceStream = new KafkaSourceStream(descriptor, partitionMetadata);
      sourceStreams.put(sourceStream.getId(), sourceStream);
    }
  }
  
  @Override
  public StorageConfig getDescriptor() { return descriptor; }

  /**
   * The stream id is equivalent to the partition id of the kafka
   */
  @Override
  public SourcePartitionStream getStream(int id) {  
    SourcePartitionStream stream = sourceStreams.get(id); 
    if(stream == null) {
      throw new RuntimeException("Cannot find the partition " + id + ", available streams = " + sourceStreams.size());
    }
    return stream;
  }

  @Override
  public SourcePartitionStream getStream(PartitionConfig descriptor) {
    return getStream(descriptor.getPartitionId());
  }

  @Override
  public SourcePartitionStream[] getStreams() {
    SourcePartitionStream[] array = new SourcePartitionStream[sourceStreams.size()];
    return sourceStreams.values().toArray(array);
  }
  
  public void close() throws Exception {
  }
  
  static public StorageConfig createStorageDescriptor(String name, String topic, String zkConnect, String reader) {
    StorageConfig descriptor = new StorageConfig("kafka");
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