package com.neverwinterdp.scribengin.storage.es.sink;

import java.util.LinkedHashMap;

import com.neverwinterdp.es.ESObjectClient;
import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.PartitionConfig;
import com.neverwinterdp.scribengin.storage.es.ESStorage;
import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStream;

public class ESSink implements Sink {
  private ESStorage storage ;
  
  private int idTracker = 0;
  private LinkedHashMap<Integer, ESSinkStream> streams = new LinkedHashMap<Integer, ESSinkStream>() ;
  
  public ESSink(StorageConfig descriptor) throws Exception {
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
  public StorageConfig getDescriptor() { return storage.getStorageConfig(); }

  @Override
  public SinkPartitionStream getStream(PartitionConfig pConfig) throws Exception {
    StorageConfig sConfig = storage.getStorageConfig();
    SinkPartitionStream stream = streams.get(pConfig.getPartitionId());
    if(stream != null) return stream ;
    ESSinkStream newStream= new ESSinkStream(sConfig, pConfig) ;
    streams.put(pConfig.getPartitionId(), newStream) ;
    return newStream;
  }

  @Override
  public SinkPartitionStream[] getStreams() {
    SinkPartitionStream[] array = new SinkPartitionStream[streams.size()];
    return streams.values().toArray(array);
  }

  @Override
  public void delete(SinkPartitionStream stream) throws Exception {
    SinkPartitionStream found = streams.get(stream.getDescriptor().getPartitionId());
    if(found != null) {
      found.delete();
      streams.remove(stream.getDescriptor().getPartitionId());
    } else {
      throw new Exception("Cannot find the stream " + stream.getDescriptor().getPartitionId());
    }
  }

  @Override
  public SinkPartitionStream newStream() throws Exception {
    PartitionConfig pConfig = new PartitionConfig(storage.newStreamDescriptor());
    pConfig.setPartitionId(idTracker++);
    return new ESSinkStream(storage.getStorageConfig(), pConfig);
  }

  @Override
  public void close() throws Exception {
    for(ESSinkStream sel : streams.values()) {
    }
  }
}
