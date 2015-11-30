package com.neverwinterdp.nstorage;

import java.io.IOException;

import com.neverwinterdp.registry.RegistryException;

abstract public class NStorage {
  protected NStorageRegistry registry;
  
  protected void init(NStorageRegistry registry) {
    this.registry = registry;
  }
  
  public NStorageRegistry getRegistry() { return registry ; }
  
  public NStorageWriter getWriter(String writerId) throws RegistryException, IOException {
    return createWriter(writerId, registry);
  }
  
  abstract protected NStorageWriter createWriter(String clientId, NStorageRegistry registry) throws RegistryException, IOException;

  public NStorageReader getReader(String readerId) throws RegistryException, IOException {
    return createReader(readerId, registry);
  }
  
  abstract protected NStorageReader createReader(String clientId, NStorageRegistry registry) throws RegistryException, IOException;

  abstract public NStorageConsistencyVerifier getSegmentConsistencyVerifier() ;
}
