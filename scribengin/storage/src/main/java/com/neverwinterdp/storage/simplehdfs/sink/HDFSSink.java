package com.neverwinterdp.storage.simplehdfs.sink;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;

import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.sink.Sink;
import com.neverwinterdp.storage.sink.SinkPartitionStream;

public class HDFSSink implements Sink {
  private FileSystem    fs;
  private StorageConfig storageConfig;
  
  public HDFSSink(FileSystem fs, StorageConfig sConfig) throws FileNotFoundException, IllegalArgumentException, IOException {
    this.fs            = fs;
    this.storageConfig = sConfig;
  }
  
  public StorageConfig getStorageConfig() { return this.storageConfig; }
  
  public List<PartitionStreamConfig> getPartitionStreamConfigs() throws Exception {
    int numOfPartitionStream = storageConfig.getPartitionStream();
    List<PartitionStreamConfig> holder = new ArrayList<>();
    for(int i = 0; i < numOfPartitionStream; i++) {
      PartitionStreamConfig config = new PartitionStreamConfig(i, null);
      holder.add(config);
    }
    return holder;
  }
  
  public SinkPartitionStream  getPartitionStream(PartitionStreamConfig config) throws Exception {
    return getParitionStream(config.getPartitionStreamId());
  }
  
  public SinkPartitionStream  getParitionStream(int partitionId) throws Exception {
    PartitionStreamConfig pConfig = new PartitionStreamConfig(partitionId, null) ;
    HDFSSinkPartitionStream stream = new HDFSSinkPartitionStream(fs, storageConfig, pConfig);
    return stream ;
  }
  
  synchronized public SinkPartitionStream[] getPartitionStreams() throws Exception {
    int numOfPartitionStream = storageConfig.getPartitionStream();
    SinkPartitionStream[] stream = new SinkPartitionStream[numOfPartitionStream];
    for(int i = 0; i < numOfPartitionStream; i++) {
      stream[i] = getParitionStream(i);
    }
    return stream;
  }

  @Override
  public void close() throws Exception  { 
  }
  
  public void fsCheck() throws Exception {
  }
}