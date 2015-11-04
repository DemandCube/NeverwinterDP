package com.neverwinterdp.scribengin.storage.source;

import java.util.List;

import com.neverwinterdp.scribengin.storage.StorageConfig;

public interface Source {
  public StorageConfig getStorageConfig() ;
  public SourcePartition       getLatestSourcePartition() throws Exception ;
  public List<SourcePartition> getSourcePartitions() throws Exception ;
}
