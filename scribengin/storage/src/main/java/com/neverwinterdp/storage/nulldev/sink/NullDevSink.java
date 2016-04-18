  package com.neverwinterdp.storage.nulldev.sink;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.sink.Sink;
import com.neverwinterdp.storage.sink.SinkPartitionStream;

public class NullDevSink implements Sink {
  private StorageConfig storageConfig;
  
  
  public NullDevSink(StorageConfig descriptor) throws Exception {
    init(descriptor) ;
  }
  
  private void init(StorageConfig descriptor) throws Exception {
    this.storageConfig  = descriptor ;
  }
  
  @Override
  public StorageConfig getStorageConfig() { return storageConfig; }

  public List<PartitionStreamConfig> getPartitionStreamConfigs() throws Exception {
    List<PartitionStreamConfig> pConfigs = new ArrayList<>();
    return pConfigs ;
  }
  
  @Override
  public SinkPartitionStream getPartitionStream(PartitionStreamConfig pConfig) throws Exception {
    NullDevSinkPartitionStream newStream= new NullDevSinkPartitionStream(storageConfig, pConfig) ;
    return newStream;
  }

  @Override
  public SinkPartitionStream getPartitionStream(int partitionId) throws Exception {
    PartitionStreamConfig pConfig = new PartitionStreamConfig(partitionId);
    NullDevSinkPartitionStream newStream= new NullDevSinkPartitionStream(storageConfig, pConfig) ;
    return newStream;
  }

  
  @Override
  public SinkPartitionStream[] getPartitionStreams() throws Exception {
    List<PartitionStreamConfig> pConfigs = getPartitionStreamConfigs();
    SinkPartitionStream[] streams = new SinkPartitionStream[pConfigs.size()];
    for(int i = 0; i < pConfigs.size(); i++) {
      streams[i] = new NullDevSinkPartitionStream(storageConfig, pConfigs.get(i)) ;
    }
    return streams;
  }

  @Override
  public void close() throws Exception {
  }
}