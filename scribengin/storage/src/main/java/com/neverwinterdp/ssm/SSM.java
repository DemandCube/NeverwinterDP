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
  
  abstract protected SSMReader createReader(String clientId, SSMRegistry registry) throws RegistryException, IOException;

  public void doManagement() throws RegistryException, IOException {
    registry.doManagement();
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
