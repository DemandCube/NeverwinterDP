package com.neverwinterdp.nstorage;

import java.io.IOException;

import com.neverwinterdp.registry.RegistryException;

abstract public class NStorageReader {
  private NStorageRegistry registry;
  
  public NStorageReader(String clientId, NStorageRegistry registry) {
    this.registry = registry;
  }
  
  public boolean hasNext() throws IOException, RegistryException {
    return false;
  }
  
  public byte[] nextRecord() throws IOException, RegistryException {
    return null;
  }
  
  public void prepareCommit() throws IOException {
  }
  
  public void completeCommit() throws IOException {
  }
}