package com.neverwinterdp.nstorage.segment;

import java.io.IOException;

import com.neverwinterdp.registry.RegistryException;

abstract public class SegmentStorage {
  protected String clientId;
  protected SegmentRegistry segStorageReg;
  
  protected ReaderDescriptor reader;
  protected WriterDescriptor writer;
  
  protected void init(String clientId, SegmentRegistry segStorageReg) {
    this.clientId      = clientId;
    this.segStorageReg = segStorageReg;
  }
  
  public SegmentRegistry getSegmentStorageRegistry() { return segStorageReg ; }
  
  public SegmentReader getReader() throws RegistryException, IOException  {
    if(reader == null) {
      reader = segStorageReg.createReader(clientId);
    }
    return null;
  }
  
  public SegmentWriter newSegmentWriter() throws RegistryException, IOException  {
    if(writer == null) {
      writer = segStorageReg.createWriter(clientId);
    }
    
    SegmentDescriptor segment = segStorageReg.newSegment(writer);
    SegmentWriter segWriter = nextSegmentWriter(writer, segment) ;
    return segWriter;
  }
  
  abstract protected SegmentWriter nextSegmentWriter(WriterDescriptor writer, SegmentDescriptor segment) throws RegistryException, IOException;
  
}
