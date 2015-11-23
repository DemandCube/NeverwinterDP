package com.neverwinterdp.nstorage.segment;

import java.io.IOException;

import com.neverwinterdp.registry.RegistryException;

abstract public class SegmentWriter {
  protected String                 name;
  protected SegmentStorageRegistry registry;
  protected SegmentDescriptor      segment;
  
  public SegmentWriter(String name, SegmentStorageRegistry registry, SegmentDescriptor segment) {
    this.name     = name;
    this.registry = registry;
    this.segment  = segment;
  }

  public boolean isFull() throws IOException, RegistryException {
    return isBufferFull();
  }
  
  public void write(byte[] data) throws IOException, RegistryException {
  }
  
  public void prepareCommit() throws IOException, RegistryException {
    bufferPrepareCommit();
  }
  
  public void completeCommit() throws IOException, RegistryException {
    bufferCompleteCommit();
  }
  
  public void rollback() throws IOException, RegistryException {
    bufferRollback();
  }
  
  abstract protected boolean isBufferFull() throws IOException, RegistryException;
  
  abstract protected void bufferWrite(byte[] data) throws IOException, RegistryException ;
  
  abstract protected void bufferPrepareCommit() throws IOException ;
  abstract protected void bufferCompleteCommit() throws IOException ;
  abstract protected void bufferRollback() throws IOException ;
}
