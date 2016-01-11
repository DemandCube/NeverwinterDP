package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.runtime.master.VMMasterApp;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;

public class DataflowSubmitter {
  private ScribenginClient   scribenginClient;
  private DataflowDescriptor dflDescriptor;
  private VMDescriptor       vmDataflowMasterDescriptor;
  
  
  public DataflowSubmitter(ScribenginClient scribenginClient, DataflowDescriptor dflDescriptor) {
    this.scribenginClient = scribenginClient;
    this.dflDescriptor    = dflDescriptor;
  }
  
  public DataflowSubmitter(ScribenginClient scribenginClient, Dataflow<?, ?> dataflow) {
    this(scribenginClient, dataflow.buildDataflowDescriptor());
  }
  
  public DataflowSubmitter(Registry registry, DataflowDescriptor dflDescriptor) {
    this(new ScribenginClient(registry), dflDescriptor);
  }
  
  public VMDescriptor getVMDataflowMasterDescriptor() { return vmDataflowMasterDescriptor; }
  
  public DataflowSubmitter submit() throws Exception {
    VMClient vmClient = scribenginClient.getVMClient();
    Registry registry = scribenginClient.getRegistry();
    DataflowRegistry dflRegistry = new DataflowRegistry(registry, dflDescriptor);
    String dataflowPath = dflRegistry.getDataflowPath();
    
    VMConfig vmConfig = new VMConfig() ;
    String masterId = dflDescriptor.getId() + "-master" + dflRegistry.getMasterIdTracker().nextSeqId();
    vmConfig.
      setVmId(masterId).
      addRoles("dataflow-master").
      setRegistryConfig(vmClient.getRegistry().getRegistryConfig()).
      setVmApplication(VMMasterApp.class.getName()).
      setRequestCpuCores(dflDescriptor.getMaster().getCpuCores()).
      setRequestMemory(dflDescriptor.getMaster().getMemory()).
      setLog4jConfigUrl(dflDescriptor.getMaster().getLog4jConfigUrl()).
      addProperty("dataflow.registry.path", dataflowPath);
    vmClient.configureEnvironment(vmConfig);
    vmDataflowMasterDescriptor = vmClient.allocate(vmConfig);
    return this ;
  }
  
  public DataflowSubmitter waitForRunning(long timeout) throws Exception {
    waitForEqualOrGreaterThanStatus(timeout, DataflowLifecycleStatus.RUNNING) ;
    return this;
  }
  
  public DataflowSubmitter waitForFinish(long timeout) throws Exception {
    waitForEqualOrGreaterThanStatus(timeout, DataflowLifecycleStatus.FINISH) ;
    return this;
  }
  
  void waitForEqualOrGreaterThanStatus(long timeout, DataflowLifecycleStatus status) throws Exception {
    DataflowClient dflClient = scribenginClient.getDataflowClient(dflDescriptor.getId(), timeout);
    dflClient.waitForEqualOrGreaterThanStatus(3000, timeout, status);
  }
}