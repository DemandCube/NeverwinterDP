package com.neverwinterdp.scribengin.storage.kafka.sink;

import java.util.LinkedHashMap;

import com.neverwinterdp.kafka.KafkaClient;
import com.neverwinterdp.scribengin.storage.PartitionStreamConfig;
import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.kafka.KafkaStorage;
import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStream;

public class KafkaSink implements Sink {
  private StorageConfig storageConfig;
  private KafkaClient   kafkaClient;
  
  private int idTracker = 0;
  private LinkedHashMap<Integer, KafkaSinkPartitionStream> streams = new LinkedHashMap<Integer, KafkaSinkPartitionStream>() ;
  
  public KafkaSink(KafkaClient kafkaClient, String name, String topic) throws Exception {
    this.kafkaClient = kafkaClient;
    init(KafkaStorage.createStorageConfig(name, kafkaClient.getZkConnects(), topic)) ;
  }
  
  public KafkaSink(KafkaClient kafkaClient, StorageConfig descriptor) throws Exception {
    this.kafkaClient = kafkaClient;
    init(descriptor) ;
  }
  
  private void init(StorageConfig descriptor) throws Exception {
    descriptor.attribute("broker.list", kafkaClient.getKafkaBrokerList());
    this.storageConfig  = descriptor ;
  }
  
  @Override
  public StorageConfig getDescriptor() { return storageConfig; }

  @Override
  public SinkPartitionStream getPartitionStream(PartitionStreamConfig pConfig) throws Exception {
    SinkPartitionStream stream = streams.get(pConfig.getPartitionStreamId());
    if(stream != null) return stream ;
    KafkaSinkPartitionStream newStream= new KafkaSinkPartitionStream(pConfig) ;
    streams.put(pConfig.getPartitionStreamId(), newStream) ;
    return newStream;
  }

  @Override
  public SinkPartitionStream getParitionStream(int partitionId) throws Exception {
    SinkPartitionStream stream = streams.get(partitionId);
    return stream ;
  }

  
  @Override
  public SinkPartitionStream[] getPartitionStreams() {
    SinkPartitionStream[] array = new SinkPartitionStream[streams.size()];
    return streams.values().toArray(array);
  }

  @Override
  public void delete(SinkPartitionStream stream) throws Exception {
    SinkPartitionStream found = streams.get(stream.getPartitionStreamId());
    if(found != null) {
      found.delete();
      streams.remove(stream.getPartitionStreamId());
    } else {
      throw new Exception("Cannot find the stream " + stream.getPartitionStreamId());
    }
  }

  @Override
  public SinkPartitionStream newStream() throws Exception {
    PartitionStreamConfig streamDescriptor = new PartitionStreamConfig(storageConfig);
    streamDescriptor.setPartitionStreamId(idTracker++);
    return new KafkaSinkPartitionStream(streamDescriptor);
  }

  @Override
  public void close() throws Exception {
    for(KafkaSinkPartitionStream sel : streams.values()) {
    }
  }
}
