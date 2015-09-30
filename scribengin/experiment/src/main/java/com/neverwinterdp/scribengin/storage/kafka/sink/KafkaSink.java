package com.neverwinterdp.scribengin.storage.kafka.sink;

import java.util.LinkedHashMap;

import com.neverwinterdp.kafka.tool.KafkaTool;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.kafka.KafkaStorage;
import com.neverwinterdp.scribengin.storage.PartitionDescriptor;
import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStream;

public class KafkaSink implements Sink {
  private StorageDescriptor descriptor;
  
  private int idTracker = 0;
  private LinkedHashMap<Integer, KafkaSinkPartitionStream> streams = new LinkedHashMap<Integer, KafkaSinkPartitionStream>() ;
  
  public KafkaSink(StorageDescriptor descriptor) throws Exception {
    init(descriptor) ;
  }
  
  private void init(StorageDescriptor descriptor) throws Exception {
    KafkaTool kafkaTool = KafkaStorage.getKafkaTool(descriptor) ;
    descriptor.attribute("broker.list", kafkaTool.getKafkaBrokerList());
    this.descriptor  = descriptor ;
    kafkaTool.close();
  }
  
  @Override
  public StorageDescriptor getDescriptor() { return descriptor; }

  @Override
  public SinkPartitionStream getStream(PartitionDescriptor descriptor) throws Exception {
    SinkPartitionStream stream = streams.get(descriptor.getId());
    if(stream != null) return stream ;
    KafkaSinkPartitionStream newStream= new KafkaSinkPartitionStream(descriptor) ;
    streams.put(descriptor.getId(), newStream) ;
    return newStream;
  }

  @Override
  public SinkPartitionStream[] getStreams() {
    SinkPartitionStream[] array = new SinkPartitionStream[streams.size()];
    return streams.values().toArray(array);
  }

  @Override
  public void delete(SinkPartitionStream stream) throws Exception {
    SinkPartitionStream found = streams.get(stream.getDescriptor().getId());
    if(found != null) {
      found.delete();
      streams.remove(stream.getDescriptor().getId());
    } else {
      throw new Exception("Cannot find the stream " + stream.getDescriptor().getId());
    }
  }

  @Override
  public SinkPartitionStream newStream() throws Exception {
    PartitionDescriptor streamDescriptor = new PartitionDescriptor(this.descriptor);
    streamDescriptor.setId(idTracker++);
    return new KafkaSinkPartitionStream(streamDescriptor);
  }

  @Override
  public void close() throws Exception {
    for(KafkaSinkPartitionStream sel : streams.values()) {
    }
  }
}
