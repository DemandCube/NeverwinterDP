package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.runtime.master.VMMasterApp;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;

public class DataflowSubmitter {
  private ScribenginClient scribenginClient;
  private DataflowDescriptor   dflConfig;
  
  
  public DataflowSubmitter(ScribenginClient scribenginClient, DataflowDescriptor dflConfig) {
    this.scribenginClient = scribenginClient;
    this.dflConfig = dflConfig;
  }
  
  public DataflowSubmitter(ScribenginClient scribenginClient, Dataflow<?, ?> dataflow) {
    this(scribenginClient, dataflow.buildDataflowDescriptor());
  }
  
  public DataflowSubmitter(Registry registry, DataflowDescriptor dflConfig) {
    this(new ScribenginClient(registry), dflConfig);
  }
  
  public DataflowSubmitter submit() throws Exception {
    VMClient vmClient = scribenginClient.getVMClient();
    Registry registry = scribenginClient.getRegistry();
    DataflowRegistry dflRegistry = new DataflowRegistry();
    String dataflowPath = dflRegistry.create(registry, dflConfig);
    VMConfig vmConfig = new VMConfig() ;
    vmConfig.
      setVmId(dflConfig.getId() + "-master").
      addRoles("dataflow-master").
      setRegistryConfig(vmClient.getRegistry().getRegistryConfig()).
      setVmApplication(VMMasterApp.class.getName()).
      setRequestCpuCores(dflConfig.getMaster().getCpuCores()).
      setRequestMemory(dflConfig.getMaster().getMemory()).
      setLog4jConfigUrl(dflConfig.getMaster().getLog4jConfigUrl()).
      addProperty("dataflow.registry.path", dataflowPath);
    vmClient.configureEnvironment(vmConfig);
    VMDescriptor vmDescriptor = vmClient.allocate(vmConfig);
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
    DataflowClient dflClient = scribenginClient.getDataflowClient(dflConfig.getId(), timeout);
    dflClient.waitForEqualOrGreaterThanStatus(3000, timeout, status);
  }
}
