package com.neverwinterdp.ssm;

import java.io.IOException;
import java.util.List;

import com.neverwinterdp.registry.RegistryException;

abstract public class SSM {
  protected SSMRegistry registry;
  
  protected void init(SSMRegistry registry) {
    this.registry = registry;
  }
  
  public SSMRegistry getRegistry() { return registry ; }
  
  public SSMWriter getWriter(String writerId) throws RegistryException, IOException {
    return createWriter(writerId, registry);
  }
  
  abstract protected SSMWriter createWriter(String clientId, SSMRegistry registry) throws RegistryException, IOException;

  public SSMReader getReader(String readerId) throws RegistryException, IOException {
    return createReader(readerId, registry);
  }
  
  public SSMReader getReader(String readerId, int startFromSegmentId, long recordPos) throws RegistryException, IOException {
    return createReader(readerId, registry, startFromSegmentId, recordPos);
  }
  
  abstract protected SSMReader createReader(String clientId, SSMRegistry registry) throws RegistryException, IOException;

  abstract protected SSMReader createReader(String clientId, SSMRegistry registry, int segmentId, long recordPos) throws RegistryException, IOException;

  
  public void doManagement() throws RegistryException, IOException {
    registry.doManagement();
  }
  
  public void dropSegmentByTag(SSMTagDescriptor tag) throws RegistryException, IOException {
    List<String> segments = registry.getSegments();
    for(int i = 0; i < segments.size(); i++) {
      String segmentIdName = segments.get(i);
      int segmentId = SegmentDescriptor.extractId(segmentIdName);
      if(segmentId < tag.getSegmentId()) {
        registry.deleteSegment(segmentIdName);
        doDeleteSegment(segmentIdName);
      } else {
        break;
      }
    }
  }
  
  public void cleanReadSegmentByActiveReader() throws RegistryException, IOException {
    List<String> cleanSegments = registry.cleanReadSegmentByActiveReader();
    for(int i = 0; i < cleanSegments.size(); i++) {
      String segmentId = cleanSegments.get(i);
      doDeleteSegment(segmentId);
    }
  }
  
  abstract protected void doDeleteSegment(String segmentId) throws IOException;
  
  abstract public SSMConsistencyVerifier getSegmentConsistencyVerifier() ;
  
}
