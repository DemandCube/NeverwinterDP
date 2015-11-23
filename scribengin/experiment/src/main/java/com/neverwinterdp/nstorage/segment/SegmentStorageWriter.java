package com.neverwinterdp.nstorage.segment;

import java.io.IOException;

import com.neverwinterdp.registry.RegistryException;

abstract public class SegmentStorageWriter {
  protected SegmentStorageRegistry segStorageReg;
  private   SegmentWriter          currentWriter ;
  
  protected SegmentStorageWriter(SegmentStorageRegistry segStorageReg) {
    this.segStorageReg = segStorageReg;
  }
  
  abstract protected SegmentWriter nextSegmentWriter(SegmentDescriptor segment) throws RegistryException, IOException;
  
  private SegmentWriter nextSegmentWriter() throws RegistryException, IOException  {
    SegmentDescriptor segment = segStorageReg.newSegment();
    return nextSegmentWriter(segment) ;
  }
  
  public void write(byte[] data) throws RegistryException, IOException {
    if(currentWriter == null) {
      
    } else if(currentWriter.isBufferFull()) {
      
    }
  }
}
