package com.neverwinterdp.nstorage.segment;

abstract public class SegmentStorage {
  protected SegmentStorageRegistry registry;
  
  protected void init(SegmentStorageRegistry registry) {
    this.registry = registry;
  }
  
  public SegmentStorageRegistry getSegmentStorageRegistry() { return this.registry ; }
}
