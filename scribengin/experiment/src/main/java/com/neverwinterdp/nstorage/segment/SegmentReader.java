package com.neverwinterdp.nstorage.segment;

import java.io.IOException;

import com.neverwinterdp.registry.RegistryException;

abstract public class SegmentReader {
  private SegmentStorageRegistry registry;
  
  public SegmentReader(SegmentStorageRegistry registry) {
    this.registry = registry;
  }
  
  abstract public void readFully(byte[] data) throws IOException, RegistryException ;
}
