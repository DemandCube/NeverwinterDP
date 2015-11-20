package com.neverwinterdp.nstorage.segment;

import java.io.IOException;

import com.neverwinterdp.registry.RegistryException;

abstract public class SegmentWriter {
  private String                 name;
  private SegmentStorageRegistry registry;
  private SegmentDescriptor      segment;
  
  public SegmentWriter(String name, SegmentStorageRegistry registry, SegmentDescriptor segment) {
    this.name     = name;
    this.registry = registry;
    this.segment  = segment;
  }

  abstract public boolean isFull() throws IOException, RegistryException;
  
  abstract public void write(byte[] data) throws IOException, RegistryException ;
}
