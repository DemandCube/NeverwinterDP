package com.neverwinterdp.scribengin;

import java.util.List;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.dataflow.DataflowClient;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.runtime.master.VMMasterApp;
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
  
  public DataflowClient getDataflowClient(String dataflowId) throws Exception {
    return getDataflowClient(dataflowId,DataflowLifecycleStatus.RUNNING, 1000, 60000) ;
  }
  
  public DataflowClient getDataflowClient(String dataflowId, long timeout) throws Exception {
    return getDataflowClient(dataflowId,DataflowLifecycleStatus.RUNNING, 1000, timeout) ;
  }
  
  public DataflowClient getDataflowClient(String dataflowId, DataflowLifecycleStatus exepectStatus, long checkPeriod, long timeout) throws Exception {
    Registry registry = getRegistry() ;
    String dataflowPath = DataflowRegistry.DATAFLOW_ALL_PATH + "/" + dataflowId;
    DataflowLifecycleStatus status = null;
    long stopTime = System.currentTimeMillis() + timeout;
    while(System.currentTimeMillis() < stopTime) {
      String statusPath = dataflowPath + "/" + DataflowRegistry.DATAFLOW_STATUS;
      if(getRegistry().exists(statusPath)) {
        status = registry.get(statusPath).getDataAs(DataflowLifecycleStatus.class) ;
        if(status != null && status.equalOrGreaterThan(exepectStatus)) {
          DataflowClient dataflowClient = new DataflowClient(this, dataflowPath);
          return dataflowClient ;
        }
      }
      Thread.sleep(checkPeriod);
    }
    throw new Exception("The dataflow " + dataflowId + " is " +  status + " after " + timeout + "ms");
  }
  
  public VMDescriptor submit(DataflowDescriptor dflDescriptor) throws Exception {
    DataflowRegistry dflRegistry = new DataflowRegistry(getRegistry(), dflDescriptor);
    return submit(dflRegistry, dflDescriptor);
  }

  public DataflowClient resume(String dataflowId, long timeout) throws Exception {
    String dataflowPath = DataflowRegistry.DATAFLOW_ALL_PATH + "/" + dataflowId;
    DataflowRegistry dflRegistry = new DataflowRegistry(getRegistry(), dataflowPath) ;
    DataflowDescriptor dflDescriptor = dflRegistry.getConfigRegistry().getDataflowDescriptor();
    submit(dflRegistry, dflDescriptor);
    return getDataflowClient(dataflowId, timeout);
  }
  
  VMDescriptor submit( DataflowRegistry dflRegistry, DataflowDescriptor dflDescriptor) throws Exception {
    VMClient vmClient = getVMClient();
    String dataflowPath = dflRegistry.getDataflowPath();
    
    VMConfig vmConfig = new VMConfig() ;
    String masterId = dflDescriptor.getId() + "-master-" + dflRegistry.getMasterIdTracker().nextSeqId();
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
    VMDescriptor vmDataflowMasterDescriptor = vmClient.allocate(vmConfig);
    return vmDataflowMasterDescriptor ;
  }
}