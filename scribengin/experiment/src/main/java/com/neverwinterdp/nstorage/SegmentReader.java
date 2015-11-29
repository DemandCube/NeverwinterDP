package com.neverwinterdp.nstorage;

import java.io.IOException;

import com.neverwinterdp.registry.RegistryException;

abstract public class SegmentReader {
  private NStorageRegistry         registry;
  private NStorageReaderDescriptor readerDescriptor;
  private SegmentDescriptor        segment;
  
  public SegmentReader(NStorageRegistry registry,NStorageReaderDescriptor readerDescriptor, SegmentDescriptor segment) {
    this.registry         = registry;
    this.readerDescriptor = readerDescriptor;
    this.segment          = segment;
  }
  
  public boolean hasNext() throws IOException, RegistryException {
    return getCurrentReadPosition() < segment.getDataSegmentLastCommitPos();
  }
  
  public byte[] nextRecord() throws IOException, RegistryException {
    return dataNextRecord();
  }
  
  abstract protected byte[] dataNextRecord() throws IOException;
  
  public void prepareCommit() throws IOException {
  }
  
  public void completeCommit() throws IOException {
  }
  
  public void rollback() throws IOException {
  }
  
  abstract protected long getCurrentReadPosition() ;
}