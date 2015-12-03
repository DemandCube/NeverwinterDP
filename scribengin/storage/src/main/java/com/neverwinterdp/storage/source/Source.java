package com.neverwinterdp.storage.source;

import java.util.List;

import com.neverwinterdp.storage.StorageConfig;

public interface Source {
  public StorageConfig getStorageConfig() ;
  public SourcePartition getLatestSourcePartition() throws Exception ;
  public List<? extends SourcePartition> getSourcePartitions() throws Exception ;
}
