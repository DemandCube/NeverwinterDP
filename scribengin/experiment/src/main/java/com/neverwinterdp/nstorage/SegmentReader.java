package com.neverwinterdp.nstorage;

import java.io.IOException;

import com.neverwinterdp.registry.RegistryException;

abstract public class SegmentReader {
  private NStorageRegistry  registry;
  private SegmentDescriptor segment;
  
  public SegmentReader(NStorageRegistry registry, SegmentDescriptor segment) {
    this.registry = registry;
    this.segment  = segment;
  }
  
  public boolean hasNext() throws IOException, RegistryException {
    return false;
  }
  
  public byte[] nextRecord() throws IOException, RegistryException {
    return dataNextRecord();
  }
  
  abstract protected byte[] dataNextRecord() throws IOException;
  
  public void prepareCommit() throws IOException {
  }
  
  public void completeCommit() throws IOException {
  }
}