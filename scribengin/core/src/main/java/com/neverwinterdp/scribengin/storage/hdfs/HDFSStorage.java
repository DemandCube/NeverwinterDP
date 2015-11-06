package com.neverwinterdp.scribengin.storage.hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.scribengin.storage.Storage;
import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.hdfs.sink.HDFSSink;
import com.neverwinterdp.scribengin.storage.hdfs.source.HDFSSource;

public class HDFSStorage extends Storage {
  private FileSystem             fs;

  public HDFSStorage(FileSystem fs, StorageConfig storageDescriptor) {
    super(storageDescriptor);
    this.fs = fs ;
  }

  @Override
  public void refresh() throws Exception {
  }

  @Override
  public boolean exists() throws Exception {
    String location = getStorageConfig().getLocation();
    return fs.exists(new Path(location));
  }

  @Override
  public void drop() throws Exception {
    String location = getStorageConfig().getLocation();
    fs.delete(new Path(location), true);
  }

  @Override
  public void create(int numOfPartition, int replication) throws Exception {
  }

  @Override
  public HDFSSink getSink() throws Exception {
    return new HDFSSink(fs, getStorageConfig());
  }

  @Override
  public HDFSSource getSource() throws Exception {
    return new HDFSSource(fs, getStorageConfig());
  }
}