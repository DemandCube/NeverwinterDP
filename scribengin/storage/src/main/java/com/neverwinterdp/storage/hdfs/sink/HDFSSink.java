package com.neverwinterdp.storage.hdfs.sink;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;

import com.neverwinterdp.ssm.SSMRegistry;
import com.neverwinterdp.ssm.hdfs.HdfsSSM;
import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.hdfs.HDFSStorageRegistry;
import com.neverwinterdp.storage.sink.Sink;
import com.neverwinterdp.storage.sink.SinkPartitionStream;

public class HDFSSink implements Sink {
  private HDFSStorageRegistry storageRegistry ;
  private FileSystem          fs;
  
  public HDFSSink(HDFSStorageRegistry storageRegistry) {
    this.storageRegistry = storageRegistry;
  }
  
  @Override
  public StorageConfig getStorageConfig() { return storageRegistry.getStorageConfig(); }

  @Override
  public List<PartitionStreamConfig> getPartitionStreamConfigs() throws Exception {
    StorageConfig storageConfig = getStorageConfig();
    int numOfPartitionStream = storageConfig.getPartitionStream();
    List<PartitionStreamConfig> holder = new ArrayList<>();
    for(int i = 0; i < numOfPartitionStream; i++) {
      PartitionStreamConfig config = new PartitionStreamConfig(i, null);
      holder.add(config);
    }
    return holder;
  }

  @Override
  public HDFSSinkPartitionStream getPartitionStream(PartitionStreamConfig pConfig) throws Exception {
    StorageConfig sConfig = getStorageConfig();
    String pLocation = sConfig.getLocation() + "/partition-" + pConfig.getPartitionStreamId();
    SSMRegistry pRegistry = storageRegistry.getPartitionRegistry(pConfig.getPartitionStreamId());
    HdfsSSM pStorage = new HdfsSSM(fs, pLocation, pRegistry);
    return new HDFSSinkPartitionStream(pStorage, sConfig, pConfig);
  }

  @Override
  public HDFSSinkPartitionStream getPartitionStream(int partitionId) throws Exception {
    PartitionStreamConfig config = new PartitionStreamConfig(partitionId, null);
    return getPartitionStream(config) ;
  }

  @Override
  public SinkPartitionStream[] getPartitionStreams() throws Exception {
    StorageConfig sConfig = getStorageConfig();
    int numOfPartitionStream = sConfig.getPartitionStream();
    SinkPartitionStream[] stream = new SinkPartitionStream[numOfPartitionStream];
    for(int i = 0; i < numOfPartitionStream; i++) {
      stream[i] = getPartitionStream(i);
    }
    return stream;
  }

  @Override
  public void close() throws Exception {
  }
}
