package com.neverwinterdp.scribengin;

import java.util.List;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.dataflow.config.DataflowConfig;
import com.neverwinterdp.scribengin.dataflow.master.VMDataflowServiceApp;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;

public class ScribenginClient {
  private VMClient vmClient;

  public ScribenginClient(Registry registry) {
    vmClient = new VMClient(registry);
  }
  
  public ScribenginClient(VMClient vmClient) {
    this.vmClient = vmClient;
  }

  public Registry getRegistry() { return this.vmClient.getRegistry(); }
  
  public VMClient getVMClient() { return this.vmClient; }
  
  public List<String> getActiveDataflowIds() throws RegistryException {
    return vmClient.getRegistry().getChildren(DataflowRegistry.DATAFLOW_ACTIVE_PATH) ;
  }
  
  public List<String> getHistoryDataflowIds() throws RegistryException {
    return vmClient.getRegistry().getChildren(DataflowRegistry.DATAFLOW_HISTORY_PATH) ;
  }
  
  public DataflowRegistry getDataflowRegistry(String dataflowId) throws Exception {
    String dataflowPath = DataflowRegistry.DATAFLOW_ALL_PATH + "/" + dataflowId;
    DataflowRegistry dataflowRegistry = new DataflowRegistry(getRegistry(), dataflowPath);
    return dataflowRegistry;
  }
}
