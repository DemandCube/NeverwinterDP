package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.config.DataflowConfig;
import com.neverwinterdp.scribengin.dataflow.master.VMDataflowMasterApp;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;

public class DataflowSubmitter {
  private ScribenginClient scribenginClient;
  
  public DataflowSubmitter(ScribenginClient scribenginClient) {
    this.scribenginClient = scribenginClient;
  }
  
  public void submit(DataflowConfig dflConfig) throws Exception {
    VMClient vmClient = scribenginClient.getVMClient();
    Registry registry = scribenginClient.getRegistry();
    DataflowRegistry dflRegistry = new DataflowRegistry();
    String dataflowPath = dflRegistry.create(registry, dflConfig);
    VMConfig vmConfig = new VMConfig() ;
    vmConfig.
      setName(dflConfig.getId() + "-master").
      addRoles("dataflow-master").
      setRegistryConfig(vmClient.getRegistry().getRegistryConfig()).
      setVmApplication(VMDataflowMasterApp.class.getName()).
      addProperty("dataflow.registry.path", dataflowPath);
    vmClient.configureEnvironment(vmConfig);
    VMDescriptor vmDescriptor = vmClient.allocate(vmConfig);
  }
}
