package com.neverwinterdp.scribengin.storage.es.sink;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import com.neverwinterdp.es.ESObjectClient;
import com.neverwinterdp.scribengin.storage.PartitionStreamConfig;
import com.neverwinterdp.scribengin.storage.StorageConfig;
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

  public List<PartitionStreamConfig> getPartitionStreamConfigs() throws Exception {
    int numOfPartitionStream = storage.getStorageConfig().getPartitionStream();
    List<PartitionStreamConfig> holder = new ArrayList<>();
    for(int i = 0; i < numOfPartitionStream; i++) {
      PartitionStreamConfig config = new PartitionStreamConfig(i, null);
      holder.add(config);
    }
    return holder;
  }
  
  @Override
  public SinkPartitionStream getPartitionStream(PartitionStreamConfig pConfig) throws Exception {
    StorageConfig sConfig = storage.getStorageConfig();
    SinkPartitionStream stream = streams.get(pConfig.getPartitionStreamId());
    if(stream != null) return stream ;
    ESSinkStream newStream= new ESSinkStream(sConfig, pConfig) ;
    streams.put(pConfig.getPartitionStreamId(), newStream) ;
    return newStream;
  }
  
  @Override
  public SinkPartitionStream getParitionStream(int partitionId) throws Exception {
    SinkPartitionStream stream = streams.get(partitionId);
    if(stream != null) return stream ;
    return null;
  }

  @Override
  public SinkPartitionStream[] getPartitionStreams() {
    SinkPartitionStream[] array = new SinkPartitionStream[streams.size()];
    return streams.values().toArray(array);
  }

  @Override
  public void close() throws Exception {
    for(ESSinkStream sel : streams.values()) {
    }
  }
}
