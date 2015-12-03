package com.neverwinterdp.storage.hdfs;

import org.apache.hadoop.fs.FileSystem;

import com.neverwinterdp.registry.ErrorCode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.storage.Storage;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.hdfs.sink.HDFSSink;
import com.neverwinterdp.storage.sink.Sink;
import com.neverwinterdp.storage.source.Source;

public class HDFSStorage extends Storage {
  final static public String REGISTRY_PATH = "registry.path";

  private HDFSStorageRegistry storageRegistry;
  private FileSystem          fs;

  public HDFSStorage(Registry registry, FileSystem fs, StorageConfig storageConfig) throws RegistryException {
    super(storageConfig);
    this.fs = fs ;
    this.storageRegistry = new HDFSStorageRegistry(registry, storageConfig);
  }

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
  public Sink getSink() throws Exception {
    return new HDFSSink(storageRegistry);
  }

  @Override
  public Source getSource() throws Exception {
    return null;
  }
}
