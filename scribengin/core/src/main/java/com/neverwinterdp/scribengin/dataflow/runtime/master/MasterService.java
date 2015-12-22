package com.neverwinterdp.scribengin.dataflow.runtime.master;

import org.slf4j.Logger;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.task.dedicated.DedicatedTaskService;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.runtime.DataStreamOperatorDescriptor;
import com.neverwinterdp.scribengin.dataflow.runtime.master.activity.DataflowInitActivityBuilder;
import com.neverwinterdp.scribengin.dataflow.runtime.master.activity.DataflowMasterActivityService;
import com.neverwinterdp.scribengin.dataflow.runtime.master.activity.DataflowRunActivityBuilder;
import com.neverwinterdp.scribengin.dataflow.runtime.master.activity.DataflowStopActivityBuilder;
import com.neverwinterdp.storage.StorageService;
import com.neverwinterdp.util.log.LoggerFactory;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;

@Singleton
public class MasterService {
  private Logger logger ;
  
  @Inject
  private VMConfig vmConfig;
 
  @Inject
  private DataflowRegistry dflRegistry;
  
  @Inject
  private StorageService storageService ;
  
  private DataflowWorkerMonitor workerMonitor;
  
  private DataflowTaskMonitor  taskMonitor;
  private DedicatedTaskService<DataStreamOperatorDescriptor> taskService ;
  
  public VMConfig getVMConfig() { return vmConfig; }
  
  private DataflowMasterActivityService activityService;

  public DataflowRegistry getDataflowRegistry() { return this.dflRegistry; }
  
  public StorageService getStorageService() { return storageService; }

  @Inject
  public void onInject(Injector container, LoggerFactory lfactory) throws Exception {
    logger = lfactory.getLogger(MasterService.class);
    activityService = new DataflowMasterActivityService(container, dflRegistry) ;
  }
  
  public void addWorker(VMDescriptor vmDescriptor) throws RegistryException {
    workerMonitor.addWorker(vmDescriptor);
  }
  
  public void init() throws Exception {
    System.out.println("DataflowMasterService: init()");
    dflRegistry.setStatus(DataflowLifecycleStatus.INIT);
    dflRegistry.initRegistry();
    
    workerMonitor = new DataflowWorkerMonitor(dflRegistry, activityService);
    
    taskService = new DedicatedTaskService<>(dflRegistry.getTaskRegistry(), null);
    taskMonitor = new DataflowTaskMonitor();
    taskService.addTaskMonitor(taskMonitor);
    System.out.println("DataflowMasterService: init(), done!!!");
  }
  
  public void run() throws Exception {
    System.out.println("DataflowMasterService: run()");
    activityService.queue(new DataflowInitActivityBuilder().build());
    activityService.queue(new DataflowRunActivityBuilder().build());
  }
  
  public void waitForTermination() throws Exception {
    long maxRunTime = dflRegistry.getConfigRegistry().getDataflowDescriptor().getMaxRunTime();
    System.out.println("DataflowMasterService: waitForTermination()");
    taskMonitor.waitForAllTaskFinish(maxRunTime);
    activityService.queue(new DataflowStopActivityBuilder().build());
    workerMonitor.waitForAllWorkerTerminated();
    taskMonitor.waitForAllTaskFinish(30000);
    System.out.println("DataflowMasterService: waitForTermination(), wait for all task terminate");
    
    taskService.onDestroy();
    System.out.println("DataflowMasterService: waitForTermination(), taskService.onDestroy();");
    activityService.onDestroy();
    System.out.println("DataflowMasterService: waitForTermination(), activityService.onDestroy()");
    //finish
    dflRegistry.setStatus(DataflowLifecycleStatus.FINISH);
    System.out.println("DataflowMasterService: waitForTermination(), done!!!");
  }
}
