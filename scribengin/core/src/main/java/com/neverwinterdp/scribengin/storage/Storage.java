package com.neverwinterdp.scribengin.storage;

import java.util.List;

import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.source.SourcePartition;

abstract public class Storage {
  private StorageConfig storageConfig;
  
  public Storage(StorageConfig storageDescriptor) {
    this.storageConfig = storageDescriptor ;
  }
  
  public StorageConfig getStorageConfig() { return this.storageConfig; }
  
  abstract public List<PartitionStreamConfig> getPartitionConfigs() throws Exception ;
  
  abstract public void refresh() throws Exception ; 
  
  abstract public boolean exists() throws Exception ;
  
  abstract public void drop() throws Exception ;
  abstract public void create(int numOfPartition, int replication) throws Exception;
  
  abstract public Sink getSink() throws Exception ;
  abstract public SourcePartition getSource() throws Exception ;
}
