package com.neverwinterdp.scribengin.storage.es.sink;

import java.util.LinkedHashMap;

import com.neverwinterdp.es.ESObjectClient;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.es.ESStorage;
import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.sink.SinkStream;

public class ESSink implements Sink {
  private ESStorage storage ;
  
  private int idTracker = 0;
  private LinkedHashMap<Integer, ESSinkStream> streams = new LinkedHashMap<Integer, ESSinkStream>() ;
  
  public ESSink(StorageDescriptor descriptor) throws Exception {
    init(new ESStorage(descriptor)) ;
  }
  
  public ESSink(String[] address, String indexName, Class<?> mappingType) throws Exception {
    init(new ESStorage(address, indexName, mappingType));
  }
  
  private void init(ESStorage storage) throws Exception {
    this.storage = storage;
    ESObjectClient<Object> esObjClient = storage.getESObjectClient();
    if(!esObjClient.isCreated()) {
      esObjClient.createIndexWith(null, null);
    }
    esObjClient.close();
  }
  
  @Override
  public StorageDescriptor getDescriptor() { return storage.getStorageDescriptor(); }

  @Override
  public SinkStream getStream(StreamDescriptor descriptor) throws Exception {
    SinkStream stream = streams.get(descriptor.getId());
    if(stream != null) return stream ;
    ESSinkStream newStream= new ESSinkStream(descriptor) ;
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
    StreamDescriptor streamDescriptor = new StreamDescriptor(storage.newStreamDescriptor());
    streamDescriptor.setId(idTracker++);
    return new ESSinkStream(streamDescriptor);
  }

  @Override
  public void close() throws Exception {
    for(ESSinkStream sel : streams.values()) {
    }
  }
}
