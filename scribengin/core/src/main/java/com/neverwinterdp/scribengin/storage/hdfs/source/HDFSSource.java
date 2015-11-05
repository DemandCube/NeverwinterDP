package com.neverwinterdp.scribengin.storage.hdfs.source;

import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.source.Source;
import com.neverwinterdp.scribengin.storage.source.SourcePartition;

public class HDFSSource implements Source {
  private FileSystem    fs;
  private StorageConfig storageConfig;
  
  public HDFSSource(FileSystem fs, String location) throws Exception {
    this.fs = fs ;
    this.storageConfig = new StorageConfig("HDFS", location);
  }
  
  public HDFSSource(FileSystem fs, StorageConfig storageConfig) throws Exception {
    this.fs = fs ;
    this.storageConfig = storageConfig ;
  }
  
  @Override
  public StorageConfig getStorageConfig() { return storageConfig; }

  @Override
  public SourcePartition getLatestSourcePartition() throws Exception {
    return null;
  }

  @Override
  public List<SourcePartition> getSourcePartitions() throws Exception {
    return null;
  }

  public void refresh() throws Exception {
    Path fsLoc = new Path(storageConfig.getLocation());
    if(!fs.exists(fsLoc)) {
      throw new Exception("location " + storageConfig.getLocation() + " does not exist!") ;
    }
    
    FileStatus[] status = fs.listStatus(new Path(storageConfig.getLocation())) ;
    for(FileStatus sel : status) {
    }
  }
}
