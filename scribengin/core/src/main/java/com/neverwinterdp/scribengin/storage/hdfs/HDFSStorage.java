package com.neverwinterdp.scribengin.storage.hdfs;

import org.apache.hadoop.fs.FileSystem;

import com.neverwinterdp.scribengin.storage.Storage;
import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.source.Source;

public class HDFSStorage extends Storage {
  private FileSystem    fs ;
  private StorageConfig storageConfig;
  
  public HDFSStorage(FileSystem fs, StorageConfig storageDescriptor) {
    super(storageDescriptor);
    this.fs = fs ;
  }

  @Override
  public void refresh() throws Exception {
  }

  @Override
  public boolean exists() throws Exception {
    return false;
  }

  @Override
  public void drop() throws Exception {
  }

  @Override
  public void create(int numOfPartition, int replication) throws Exception {
  }

  @Override
  public Sink getSink() throws Exception {
    return null;
  }

  @Override
  public Source getSource() throws Exception {
    return null;
  }
}
