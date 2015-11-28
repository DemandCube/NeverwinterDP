package com.neverwinterdp.nstorage.segment;

import java.io.IOException;

import com.neverwinterdp.registry.RegistryException;

abstract public class SegmentStorage {
  protected String clientId;
  protected SegmentRegistry segRegistry;
  
  protected ReaderDescriptor reader;
  protected WriterDescriptor writer;
  
  protected void init(String clientId, SegmentRegistry segStorageReg) {
    this.clientId      = clientId;
    this.segRegistry = segStorageReg;
  }
  
  public SegmentRegistry getSegmentRegistry() { return segRegistry ; }
  
  public SegmentReader getReader() throws RegistryException, IOException  {
    if(reader == null) {
      reader = segRegistry.createReader(clientId);
    }
    return null;
  }
  
  public SegmentWriter newSegmentWriter() throws RegistryException, IOException  {
    if(writer == null) {
      writer = segRegistry.createWriter(clientId);
    }
    
    SegmentDescriptor segment = segRegistry.newSegment(writer);
    SegmentWriter segWriter = nextSegmentWriter(writer, segment) ;
    return segWriter;
  }
  
  abstract protected SegmentWriter nextSegmentWriter(WriterDescriptor writer, SegmentDescriptor segment) throws RegistryException, IOException;
  
  abstract public SegmentConsistencyVerifier getSegmentConsistencyVerifier() ;
}
