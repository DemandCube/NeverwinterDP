package com.neverwinterdp.nstorage;

import java.io.IOException;

import com.neverwinterdp.registry.RegistryException;

abstract public class NStorage {
  protected String           clientId;
  protected NStorageRegistry registry;
  
  protected NStorageWriter   storageWriter;
  protected NStorageReader   storageReader;
  
  protected NStorageReaderDescriptor reader;
  protected NStorageWriterDescriptor writer;
  
  protected void init(String clientId, NStorageRegistry registry) {
    this.clientId = clientId;
    this.registry = registry;
  }
  
  public NStorageRegistry getRegistry() { return registry ; }
  
  public NStorageWriter getWriter() throws RegistryException, IOException {
    if(storageWriter == null) {
      storageWriter = createWriter(clientId, registry);
    }
    return storageWriter;
  }
  
  abstract protected NStorageWriter createWriter(String clientId, NStorageRegistry registry) throws RegistryException, IOException;

  public NStorageReader getReader() throws RegistryException, IOException {
    if(storageReader == null) {
      storageReader = createReader(clientId, registry);
    }
    return storageReader;
  }
  
  abstract protected NStorageReader createReader(String clientId, NStorageRegistry registry) throws RegistryException, IOException;

  abstract public NStorageConsistencyVerifier getSegmentConsistencyVerifier() ;
}
