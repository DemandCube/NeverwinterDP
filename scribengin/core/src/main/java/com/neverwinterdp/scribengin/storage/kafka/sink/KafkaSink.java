package com.neverwinterdp.scribengin.storage.kafka.sink;

import java.util.LinkedHashMap;

import com.neverwinterdp.kafka.KafkaClient;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.sink.SinkStream;

public class KafkaSink implements Sink {
  private KafkaClient kafkaClient;
  private StorageDescriptor descriptor;
  
  private int idTracker = 0;
  private LinkedHashMap<Integer, KafkaSinkStream> streams = new LinkedHashMap<Integer, KafkaSinkStream>() ;
  
  public KafkaSink(KafkaClient kafkaClient, StorageDescriptor descriptor) throws Exception {
    this.kafkaClient = kafkaClient;
    init(descriptor) ;
  }
  
  public KafkaSink(KafkaClient kafkaClient, String name, String topic) throws Exception {
    this.kafkaClient = kafkaClient;
    StorageDescriptor descriptor = new StorageDescriptor("kafka");
    descriptor.attribute("name", name);
    descriptor.attribute("topic", topic);
    descriptor.attribute("zk.connect", kafkaClient.getZkConnects());
    init(descriptor);
  }
  
  private void init(StorageDescriptor descriptor) throws Exception {
    descriptor.attribute("broker.list", kafkaClient.getKafkaBrokerList());
    this.descriptor  = descriptor ;
  }
  
  @Override
  public StorageDescriptor getDescriptor() { return descriptor; }

  @Override
  public SinkStream getStream(StreamDescriptor descriptor) throws Exception {
    SinkStream stream = streams.get(descriptor.getId());
    if(stream != null) return stream ;
    KafkaSinkStream newStream= new KafkaSinkStream(kafkaClient, descriptor) ;
    streams.put(descriptor.getId(), newStream) ;
    return newStream;
  }

  @Override
  public SinkStream[] getStreams() {
    SinkStream[] array = new SinkStream[streams.size()];
    return streams.values().toArray(array);
  }

  @Override
  public void delete(SinkStream stream) throws Exception {
    SinkStream found = streams.get(stream.getDescriptor().getId());
    if(found != null) {
      found.delete();
      streams.remove(stream.getDescriptor().getId());
    } else {
      throw new Exception("Cannot find the stream " + stream.getDescriptor().getId());
    }
  }

  @Override
  public SinkStream newStream() throws Exception {
    StreamDescriptor streamDescriptor = new StreamDescriptor(this.descriptor);
    streamDescriptor.setId(idTracker++);
    return new KafkaSinkStream(kafkaClient, streamDescriptor);
  }

  @Override
  public void close() throws Exception {
    for(KafkaSinkStream sel : streams.values()) {
    }
  }
}
