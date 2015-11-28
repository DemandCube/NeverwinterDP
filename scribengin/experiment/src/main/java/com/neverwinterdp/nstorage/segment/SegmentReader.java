package com.neverwinterdp.nstorage.segment;

import java.io.IOException;

import com.neverwinterdp.registry.RegistryException;

abstract public class SegmentReader {
  private SegmentRegistry   segRegistry;
  private SegmentDescriptor segment;
  
  public SegmentReader(SegmentRegistry segReg, SegmentDescriptor segment) {
    this.segRegistry = segReg;
    this.segment     = segment;
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