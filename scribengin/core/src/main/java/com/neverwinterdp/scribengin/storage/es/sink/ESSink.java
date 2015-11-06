package com.neverwinterdp.scribengin.storage.es.sink;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.es.ESObjectClient;
import com.neverwinterdp.scribengin.storage.PartitionStreamConfig;
import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.es.ESStorage;
import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStream;

public class ESSink implements Sink {
  private ESStorage storage ;
  
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
    ESSinkStream newStream= new ESSinkStream(sConfig, pConfig) ;
    return newStream;
  }
  
  @Override
  public SinkPartitionStream getParitionStream(int partitionId) throws Exception {
    StorageConfig sConfig = storage.getStorageConfig();
    PartitionStreamConfig pConfig = new PartitionStreamConfig(partitionId, null);
    ESSinkStream newStream= new ESSinkStream(sConfig, pConfig) ;
    return newStream;

  }

  @Override
  public SinkPartitionStream[] getPartitionStreams() throws Exception {
    StorageConfig sConfig = storage.getStorageConfig();
    int numOfStream = sConfig.getPartitionStream();
    SinkPartitionStream[] array = new SinkPartitionStream[numOfStream];
    for(int i = 0; i < array.length; i++) {
      array[i] = getParitionStream(i);
    }
    return array;
  }

  @Override
  public void close() throws Exception {
    
  }
}
