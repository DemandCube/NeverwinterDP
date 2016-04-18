package com.neverwinterdp.storage.es.sink;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.es.ESStorage;
import com.neverwinterdp.storage.sink.Sink;
import com.neverwinterdp.storage.sink.SinkPartitionStream;

public class ESSink implements Sink {
  private ESStorage esStorage ;
  
  public ESSink(ESStorage esStorage) {
    this.esStorage = esStorage;
  }
  
  @Override
  public StorageConfig getStorageConfig() { return esStorage.getStorageConfig(); }

  public List<PartitionStreamConfig> getPartitionStreamConfigs() throws Exception {
    int numOfPartitionStream = esStorage.getStorageConfig().getPartitionStream();
    List<PartitionStreamConfig> holder = new ArrayList<>();
    for(int i = 0; i < numOfPartitionStream; i++) {
      PartitionStreamConfig config = new PartitionStreamConfig(i, null);
      holder.add(config);
    }
    return holder;
  }
  
  @Override
  public SinkPartitionStream getPartitionStream(PartitionStreamConfig pConfig) throws Exception {
    return new ESSinkStream(esStorage, pConfig) ;
  }
  
  @Override
  public SinkPartitionStream getPartitionStream(int partitionId) throws Exception {
    PartitionStreamConfig pConfig = new PartitionStreamConfig(partitionId, null);
    ESSinkStream newStream= new ESSinkStream(esStorage, pConfig) ;
    return newStream;

  }

  @Override
  public SinkPartitionStream[] getPartitionStreams() throws Exception {
    StorageConfig sConfig = esStorage.getStorageConfig();
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
