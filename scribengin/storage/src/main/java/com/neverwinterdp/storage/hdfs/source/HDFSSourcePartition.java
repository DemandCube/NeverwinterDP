package com.neverwinterdp.storage.hdfs.source;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.ssm.SSMRegistry;
import com.neverwinterdp.ssm.hdfs.HdfsSSM;
import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.hdfs.HDFSStorageRegistry;
import com.neverwinterdp.storage.source.SourcePartition;
import com.neverwinterdp.storage.source.SourcePartitionStream;

/**
 * @author Tuan Nguyen
 */
public class HDFSSourcePartition implements SourcePartition {
  private HDFSStorageRegistry storageRegistry ;
  private FileSystem          fs;
  
  public HDFSSourcePartition(HDFSStorageRegistry storageRegistry, FileSystem fs) {
    this.storageRegistry = storageRegistry;
    this.fs = fs;
  }
  
  public String getPartitionLocation() { 
    return getStorageConfig().getLocation(); 
  }
  
  public StorageConfig getStorageConfig() { 
    return storageRegistry.getStorageConfig(); 
  }

  @Override
  public HDFSSourcePartitionStream getPartitionStream(int partitionId) throws Exception {
    PartitionStreamConfig config = new PartitionStreamConfig(partitionId, null);
    return getPartitionStream(config) ;
  }

  @Override
  public HDFSSourcePartitionStream getPartitionStream(PartitionStreamConfig pConfig) throws Exception {
    StorageConfig sConfig = getStorageConfig();
    String pLocation = sConfig.getLocation() + "/partition-" + pConfig.getPartitionStreamId();
    SSMRegistry pRegistry = storageRegistry.getPartitionRegistry(pConfig.getPartitionStreamId());
    HdfsSSM pStorage = new HdfsSSM(fs, pLocation, pRegistry);
    return new HDFSSourcePartitionStream(pStorage, sConfig, pConfig);
  }

  @Override
  public HDFSSourcePartitionStream[] getPartitionStreams() throws Exception {
    StorageConfig sConfig = getStorageConfig();
    int numOfPartitionStream = sConfig.getPartitionStream();
    HDFSSourcePartitionStream[] stream = new HDFSSourcePartitionStream[numOfPartitionStream];
    for(int i = 0; i < numOfPartitionStream; i++) {
      stream[i] = getPartitionStream(i);
    }
    return stream;
  }

  public void deleteReadDataByActiveReader() throws Exception {
    HDFSSourcePartitionStream[] streams = getPartitionStreams() ;
    for(HDFSSourcePartitionStream sel : streams) {
      sel.deleteReadDataByActiveReader();
    }
  }
  
  @Override
  public void close() throws Exception {
  }
}
