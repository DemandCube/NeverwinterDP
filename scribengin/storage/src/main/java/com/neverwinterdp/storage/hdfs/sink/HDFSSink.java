package com.neverwinterdp.storage.hdfs.sink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.ssm.SSMRegistry;
import com.neverwinterdp.ssm.hdfs.HdfsSSM;
import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.hdfs.HDFSStorageRegistry;
import com.neverwinterdp.storage.sink.Sink;

public class HDFSSink implements Sink {
  private HDFSStorageRegistry storageRegistry ;
  private FileSystem          fs;
  
  public HDFSSink(HDFSStorageRegistry storageRegistry, FileSystem fs) {
    this.storageRegistry = storageRegistry;
    this.fs = fs;
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
  public HDFSSinkPartitionStream getPartitionStream(PartitionStreamConfig pConfig) throws RegistryException, IOException {
    StorageConfig sConfig = getStorageConfig();
    String pLocation = sConfig.getLocation() + "/partition-" + pConfig.getPartitionStreamId();
    SSMRegistry pRegistry = storageRegistry.getPartitionRegistry(pConfig.getPartitionStreamId());
    HdfsSSM pStorage = new HdfsSSM(fs, pLocation, pRegistry);
    return new HDFSSinkPartitionStream(pStorage, sConfig, pConfig);
  }

  @Override
  public HDFSSinkPartitionStream getPartitionStream(int partitionId) throws RegistryException, IOException {
    PartitionStreamConfig config = new PartitionStreamConfig(partitionId, null);
    return getPartitionStream(config) ;
  }

  @Override
  public HDFSSinkPartitionStream[] getPartitionStreams() throws RegistryException, IOException {
    StorageConfig sConfig = getStorageConfig();
    int numOfPartitionStream = sConfig.getPartitionStream();
    HDFSSinkPartitionStream[] stream = new HDFSSinkPartitionStream[numOfPartitionStream];
    for(int i = 0; i < numOfPartitionStream; i++) {
      stream[i] = getPartitionStream(i);
    }
    return stream;
  }

  @Override
  public void close() throws RegistryException, IOException {
  }
}
