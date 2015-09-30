package com.neverwinterdp.scribengin.storage;

import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.source.Source;

abstract public class Storage {
  private StorageDescriptor storageDescriptor;
  
  public Storage(StorageDescriptor storageDescriptor) {
    this.storageDescriptor = storageDescriptor ;
  }
  
  public StorageDescriptor getStorageDescriptor() { return this.storageDescriptor; }
  
  abstract public void refresh() throws Exception ; 
  
  abstract public boolean exists() throws Exception ;
  
  abstract public void drop() throws Exception ;
  abstract public void create(int numOfPartition, int replication) throws Exception;
  
  abstract public Sink getSink() throws Exception ;
  abstract public Source getSource() throws Exception ;
}
