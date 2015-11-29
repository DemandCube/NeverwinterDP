package com.neverwinterdp.nstorage;

import java.io.IOException;

import com.neverwinterdp.registry.RegistryException;

abstract public class NStorageReader {
  protected NStorageRegistry registry;
  protected NStorageReaderDescriptor readerDescriptor;
  
  
  public NStorageReader(String clientId, NStorageRegistry registry) throws RegistryException {
    this.registry = registry;
    readerDescriptor = registry.getOrCreateReader(clientId);
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
  
  abstract protected SegmentReader createSegmentReader(SegmentDescriptor segment) throws RegistryException, IOException ;
}