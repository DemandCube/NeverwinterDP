package com.neverwinterdp.storage.simplehdfs.source;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.source.Source;

public class HDFSSource implements Source {
  private FileSystem    fs;
  private StorageConfig storageConfig;
  
  public HDFSSource(FileSystem fs, StorageConfig storageConfig) throws Exception {
    this.fs = fs ;
    this.storageConfig = storageConfig ;
  }
  
  @Override
  public StorageConfig getStorageConfig() { return storageConfig; }

  @Override
  public HDFSSourcePartition getLatestSourcePartition() throws Exception {
    String[] paths = getPartitionPaths();
    return new HDFSSourcePartition(fs, storageConfig, paths[paths.length - 1]);
  }

  @Override
  public List<HDFSSourcePartition> getSourcePartitions() throws Exception {
    List<HDFSSourcePartition> holder = new ArrayList<>();
    String[] paths = getPartitionPaths();
    for(String path : paths) {
      holder.add(new HDFSSourcePartition(fs, storageConfig, path));
    }
    return holder;
  }

  public void refresh() throws Exception {
  }
  
  String[] getPartitionPaths() throws Exception {
    FileStatus[] status = fs.listStatus(new Path(storageConfig.getLocation())) ;
    String[] paths = new String[status.length];
    for(int i = 0; i < paths.length; i++) {
      paths[i] = status[i].getPath().toString();
    }
    Arrays.sort(paths);
    return paths;
  }
}
