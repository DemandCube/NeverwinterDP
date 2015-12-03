package com.neverwinterdp.storage.hdfs.source;

import java.util.List;

import org.apache.hadoop.fs.FileSystem;

import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.hdfs.HDFSStorageRegistry;
import com.neverwinterdp.storage.source.Source;
import com.neverwinterdp.storage.source.SourcePartition;

public class HDFSSource implements Source {
  private HDFSStorageRegistry storageRegistry ;
  private FileSystem          fs;
  
  public HDFSSource(HDFSStorageRegistry storageRegistry) {
    this.storageRegistry = storageRegistry;
  }
  
  @Override
  public StorageConfig getStorageConfig() { return storageRegistry.getStorageConfig(); }

  @Override
  public SourcePartition getLatestSourcePartition() throws Exception {
    return null;
  }

  @Override
  public List<? extends SourcePartition> getSourcePartitions() throws Exception {
    return null;
  }
}
