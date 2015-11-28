package com.neverwinterdp.nstorage;

import java.io.IOException;

import com.neverwinterdp.registry.RegistryException;

abstract public class NStorage {
  protected String           clientId;
  protected NStorageRegistry registry;
  
  protected NStorageReaderDescriptor reader;
  protected NStorageWriterDescriptor writer;
  
  protected void init(String clientId, NStorageRegistry registry) {
    this.clientId = clientId;
    this.registry = registry;
  }
  
  public NStorageRegistry getRegistry() { return registry ; }
  
  public SegmentReader getReader(SegmentDescriptor segment) throws RegistryException, IOException  {
    if(reader == null) {
      reader = registry.createReader(clientId);
    }
    return null;
  }
  
  public SegmentWriter newSegmentWriter() throws RegistryException, IOException  {
    if(writer == null) {
      writer = registry.createWriter(clientId);
    }
    
    SegmentDescriptor segment = registry.newSegment(writer);
    SegmentWriter segWriter = createSegmentWriter(writer, segment) ;
    return segWriter;
  }
  
  abstract protected SegmentReader createSegmentReader(NStorageReaderDescriptor reader, SegmentDescriptor segment) throws RegistryException, IOException;
  
  abstract protected SegmentWriter createSegmentWriter(NStorageWriterDescriptor writer, SegmentDescriptor segment) throws RegistryException, IOException;
  
  abstract public NStorageConsistencyVerifier getSegmentConsistencyVerifier() ;
}
