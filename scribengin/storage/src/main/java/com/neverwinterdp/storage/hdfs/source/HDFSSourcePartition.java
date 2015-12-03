package com.neverwinterdp.storage.hdfs.source;

import org.apache.hadoop.fs.FileSystem;

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
  
  public HDFSSourcePartition(HDFSStorageRegistry storageRegistry) {
    this.storageRegistry = storageRegistry;
  }
  
  public String getPartitionLocation() { 
    return getStorageConfig().getLocation(); 
  }
  
  public StorageConfig getStorageConfig() { 
    return storageRegistry.getStorageConfig(); 
  }

  @Override
  public SourcePartitionStream getPartitionStream(int id) throws Exception {
    return null;
  }

  @Override
  public SourcePartitionStream getPartitionStream(PartitionStreamConfig descriptor) throws Exception {
    return null;
  }

  @Override
  public SourcePartitionStream[] getPartitionStreams() throws Exception {
    return null;
  }

  @Override
  public void close() throws Exception {
  }
}
