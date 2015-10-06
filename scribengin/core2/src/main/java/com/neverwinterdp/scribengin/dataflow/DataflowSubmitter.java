package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.config.DataflowConfig;
import com.neverwinterdp.scribengin.dataflow.master.VMMasterApp;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;

public class DataflowSubmitter {
  private ScribenginClient scribenginClient;
  private DataflowConfig   dflConfig;
  
  public DataflowSubmitter(ScribenginClient scribenginClient, DataflowConfig dflConfig) {
    this.scribenginClient = scribenginClient;
    this.dflConfig = dflConfig;
  }
  
  public void submit() throws Exception {
    VMClient vmClient = scribenginClient.getVMClient();
    Registry registry = scribenginClient.getRegistry();
    DataflowRegistry dflRegistry = new DataflowRegistry();
    String dataflowPath = dflRegistry.create(registry, dflConfig);
    VMConfig vmConfig = new VMConfig() ;
    vmConfig.
      setName(dflConfig.getId() + "-master").
      addRoles("dataflow-master").
      setRegistryConfig(vmClient.getRegistry().getRegistryConfig()).
      setVmApplication(VMMasterApp.class.getName()).
      addProperty("dataflow.registry.path", dataflowPath);
    vmClient.configureEnvironment(vmConfig);
    VMDescriptor vmDescriptor = vmClient.allocate(vmConfig);
  }
  
  void waitForEqualOrGreaterThanStatus(long timeout, DataflowLifecycleStatus status) throws Exception {
    DataflowClient dflClient = scribenginClient.getDataflowClient(dflConfig.getId(), timeout);
    dflClient.waitForEqualOrGreaterThanStatus(3000, timeout, status);
  }
  
  public void waitForRunning(long timeout) throws Exception {
    waitForEqualOrGreaterThanStatus(timeout, DataflowLifecycleStatus.RUNNING) ;
  }
  
  public void waitForFinish(long timeout) throws Exception {
    waitForEqualOrGreaterThanStatus(timeout, DataflowLifecycleStatus.FINISH) ;
  }
}