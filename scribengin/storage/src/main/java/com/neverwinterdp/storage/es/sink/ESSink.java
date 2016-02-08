package com.neverwinterdp.storage.es.sink;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.es.ESObjectClient;
import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.es.ESStorage;
import com.neverwinterdp.storage.sink.Sink;
import com.neverwinterdp.storage.sink.SinkPartitionStream;

public class ESSink implements Sink {
  private ESStorage storage ;
  
  //public ESSink(StorageConfig descriptor) throws Exception {
  //  init(new ESStorage(descriptor)) ;
  //}
  
  public ESSink(String name, String[] address, String indexName, Class<?> mappingType, StorageConfig storageConfig) throws Exception {
    init(new ESStorage(name, address, indexName, mappingType.getCanonicalName(), storageConfig));
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
  public StorageConfig getStorageConfig() { return storage.getStorageConfig(); }

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
  public SinkPartitionStream getPartitionStream(int partitionId) throws Exception {
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
      array[i] = getPartitionStream(i);
    }
    return array;
  }

  @Override
  public void close() throws Exception {
    
  }
}
