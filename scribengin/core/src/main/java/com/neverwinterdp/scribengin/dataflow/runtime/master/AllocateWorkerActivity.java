package com.neverwinterdp.scribengin.dataflow.runtime.master;

import java.util.List;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.runtime.worker.DataflowWorkerStatus;
import com.neverwinterdp.scribengin.dataflow.runtime.worker.VMWorkerApp;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;

public class AllocateWorkerActivity implements DataflowMasterActivity  {
  MasterService service ;
  
  public AllocateWorkerActivity(MasterService service) {
    this.service = service;
  }
  
  @Override
  public void execute() throws Exception {
    allocateWorkers();
    waitForWorkerRunning();
  }
  
  void allocateWorkers() throws Exception {
    DataflowRegistry dflRegistry = service.getDataflowRegistry();
    DataflowDescriptor dflConfig = dflRegistry.getConfigRegistry().getDataflowDescriptor();
    List<String> activeWorkers = dflRegistry.getWorkerRegistry().getActiveWorkerIds();

    int numOfWorkerToAllocate = dflConfig.getWorker().getNumOfInstances();
    for(int i = activeWorkers.size(); i < numOfWorkerToAllocate; i++) {
      String workerId = dflConfig.getId() + "-worker-" + dflRegistry.getWorkerIdTracker().nextSeqId();
      allocate(dflRegistry, dflConfig, workerId);
    }
  }
  
  void allocate(DataflowRegistry dflRegistry, DataflowDescriptor dflConfig, String workerId) throws Exception {
    Registry registry = dflRegistry.getRegistry();
    RegistryConfig registryConfig = registry.getRegistryConfig();
    VMConfig vmConfig = new VMConfig();
    vmConfig.
      setClusterEnvironment(service.getVMConfig().getClusterEnvironment()).
      setVmId(workerId).
      addRoles("dataflow-worker").
      setRequestCpuCores(dflConfig.getWorker().getCpuCores()).
      setRequestMemory(dflConfig.getWorker().getMemory()).
      setRegistryConfig(registryConfig).
      setVmApplication(VMWorkerApp.class.getName()).
      addProperty("dataflow.registry.path", dflRegistry.getDataflowPath()).
      setHadoopProperties(service.getVMConfig().getHadoopProperties()).
      setLog4jConfigUrl(dflConfig.getWorker().getLog4jConfigUrl()).
      setEnableGCLog(dflConfig.getWorker().isEnableGCLog());

    String dataflowAppHome = dflConfig.getDataflowAppHome();
    if(dataflowAppHome != null) {
      vmConfig.setDfsAppHome(dataflowAppHome);
      vmConfig.addVMResource("dataflow.libs", dataflowAppHome + "/libs");
    }

    VMClient vmClient = new VMClient(registry);
    VMDescriptor vmDescriptor = vmClient.allocate(vmConfig, 90000);
    service.addWorker(vmDescriptor);
  }
  
  void waitForWorkerRunning() throws Exception {
    DataflowRegistry dflRegistry = service.getDataflowRegistry();
    DataflowDescriptor dflConfig = dflRegistry.getConfigRegistry().getDataflowDescriptor();
    long maxWait = dflConfig.getWorker().getMaxWaitForRunningStatus();
    dflRegistry.
      getWorkerRegistry().
      waitForWorkerStatus(DataflowWorkerStatus.RUNNING, 1000, maxWait);
  }
}
