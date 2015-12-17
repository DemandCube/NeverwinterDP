package com.neverwinterdp.scribengin;

import com.neverwinterdp.vm.tool.VMClusterBuilder;

public class ScribenginClusterBuilder {
  private VMClusterBuilder vmClusterBuilder ;
  private ScribenginClient scribenginClient;
  
  public ScribenginClusterBuilder(VMClusterBuilder vmClusterBuilder) {
    this.vmClusterBuilder = vmClusterBuilder ;
  }
  
  public VMClusterBuilder getVMClusterBuilder() { return vmClusterBuilder ; }
  
  public ScribenginClient getScribenginClient() { 
    if(scribenginClient == null) {
      scribenginClient = new ScribenginClient(vmClusterBuilder.getVMClient());
    }
    return this.scribenginClient ; 
  }
  
  public void clean() throws Exception {
    vmClusterBuilder.clean(); 
  }
  
  public void start() throws Exception {
    startVMMasters() ;
  }
  
  public void startVMMasters() throws Exception {
    vmClusterBuilder.start();
  }  
  
  public void shutdown() throws Exception {
    vmClusterBuilder.shutdown();
  }
}
