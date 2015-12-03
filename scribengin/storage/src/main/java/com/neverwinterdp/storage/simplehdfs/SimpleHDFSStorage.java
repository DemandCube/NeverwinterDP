package com.neverwinterdp.storage.simplehdfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.storage.Storage;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.simplehdfs.sink.HDFSSink;
import com.neverwinterdp.storage.simplehdfs.source.HDFSSource;

public class SimpleHDFSStorage extends Storage {
  private FileSystem             fs;

  public SimpleHDFSStorage(FileSystem fs, StorageConfig storageDescriptor) {
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
  public void create() throws Exception {
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