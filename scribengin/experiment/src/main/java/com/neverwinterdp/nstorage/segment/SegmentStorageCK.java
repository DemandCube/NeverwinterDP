package com.neverwinterdp.nstorage.segment;

import java.io.IOException;
import java.util.List;

import com.neverwinterdp.registry.RegistryException;

abstract public class SegmentStorageCK {
  private SegmentRegistry segRegistry;
  
  public SegmentStorageCK(SegmentRegistry segReg) {
    segRegistry = segReg;
  }
  
  public void checkConsistency() throws RegistryException, IOException {
    List<String> segments = segRegistry.getSegments() ;
    
  }
}
