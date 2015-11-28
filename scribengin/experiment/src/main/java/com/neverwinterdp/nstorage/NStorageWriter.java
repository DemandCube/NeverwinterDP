package com.neverwinterdp.nstorage;

import java.io.IOException;

import com.neverwinterdp.registry.RegistryException;

abstract public class NStorageWriter {
  protected NStorageRegistry         registry;
  protected NStorageWriterDescriptor writer;
  
  
  public NStorageWriter(String clientId, NStorageRegistry registry) {
    this.registry = registry;
  }
  
  public void write(byte[] data) throws IOException, RegistryException {
  }

  public void prepareCommit() throws IOException, RegistryException {
  }
  
  public void completeCommit() throws IOException, RegistryException {
  }

  public void commit() throws IOException, RegistryException {
    prepareCommit();
    completeCommit();
  }

  public void rollback() throws IOException, RegistryException {
  }
  
  public void close() throws IOException, RegistryException {
  }
}
