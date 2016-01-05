package com.neverwinterdp.storage.hdfs;

import org.apache.hadoop.fs.FileSystem;

import com.neverwinterdp.registry.ErrorCode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.storage.Storage;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.hdfs.sink.HDFSSink;
import com.neverwinterdp.storage.hdfs.source.HDFSSource;

public class HDFSStorage extends Storage {
  private HDFSStorageRegistry storageRegistry;
  private FileSystem          fs;

  public HDFSStorage(Registry registry, FileSystem fs, StorageConfig storageConfig) throws RegistryException {
    super(storageConfig);
    this.fs = fs ;
    storageRegistry = new HDFSStorageRegistry(registry, storageConfig);
  }

  public HDFSStorageRegistry getHDFSStorageRegistry() { return storageRegistry; }
  
  @Override
  public void refresh() throws Exception {
  }

  @Override
  public boolean exists() throws Exception {
    return storageRegistry.exists();
  }

  @Override
  public void drop() throws RegistryException {
    storageRegistry.drop();
  }

  @Override
  public void create() throws Exception {
    if(storageRegistry.exists()) {
      throw new RegistryException(ErrorCode.NodeExists, "The storage is already initialized");
    }
    storageRegistry.create();
  }

  @Override
  public HDFSSink getSink() throws Exception {
    return new HDFSSink(storageRegistry, fs);
  }

  @Override
  public HDFSSource getSource() throws Exception {
    return new HDFSSource(storageRegistry, fs);
  }
}
