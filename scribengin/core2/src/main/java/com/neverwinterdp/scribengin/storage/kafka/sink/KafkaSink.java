package com.neverwinterdp.scribengin.storage.kafka.sink;

import java.util.LinkedHashMap;

import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.kafka.KafkaStorage;
import com.neverwinterdp.kafka.tool.KafkaTool;
import com.neverwinterdp.scribengin.storage.PartitionConfig;
import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStream;

public class KafkaSink implements Sink {
  private StorageConfig descriptor;
  
  private int idTracker = 0;
  private LinkedHashMap<Integer, KafkaSinkPartitionStream> streams = new LinkedHashMap<Integer, KafkaSinkPartitionStream>() ;
  
  public KafkaSink(StorageConfig descriptor) throws Exception {
    init(descriptor) ;
  }
  
  private void init(StorageConfig descriptor) throws Exception {
    KafkaTool kafkaTool = KafkaStorage.getKafkaTool(descriptor) ;
    descriptor.attribute("broker.list", kafkaTool.getKafkaBrokerList());
    this.descriptor  = descriptor ;
  }
  
  @Override
  public StorageConfig getDescriptor() { return descriptor; }

  @Override
  public SinkPartitionStream getStream(PartitionConfig pConfig) throws Exception {
    SinkPartitionStream stream = streams.get(pConfig.getPartitionId());
    if(stream != null) return stream ;
    KafkaSinkPartitionStream newStream= new KafkaSinkPartitionStream(pConfig) ;
    streams.put(pConfig.getPartitionId(), newStream) ;
    return newStream;
  }

  @Override
  public SinkPartitionStream getStream(int partitionId) throws Exception {
    SinkPartitionStream stream = streams.get(partitionId);
    return stream ;
  }

  
  @Override
  public SinkPartitionStream[] getStreams() {
    SinkPartitionStream[] array = new SinkPartitionStream[streams.size()];
    return streams.values().toArray(array);
  }

  @Override
  public void delete(SinkPartitionStream stream) throws Exception {
    SinkPartitionStream found = streams.get(stream.getParitionConfig().getPartitionId());
    if(found != null) {
      found.delete();
      streams.remove(stream.getParitionConfig().getPartitionId());
    } else {
      throw new Exception("Cannot find the stream " + stream.getParitionConfig().getPartitionId());
    }
  }

  @Override
  public SinkPartitionStream newStream() throws Exception {
    PartitionConfig streamDescriptor = new PartitionConfig(this.descriptor);
    streamDescriptor.setPartitionId(idTracker++);
    return new KafkaSinkPartitionStream(streamDescriptor);
  }

  @Override
  public void close() throws Exception {
    for(KafkaSinkPartitionStream sel : streams.values()) {
    }
  }
}
