package com.neverwinterdp.scribengin.dataflow.master;

import org.slf4j.Logger;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.task.TaskService;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.master.activity.DataflowInitActivityBuilder;
import com.neverwinterdp.scribengin.dataflow.master.activity.DataflowMasterActivityService;
import com.neverwinterdp.scribengin.dataflow.master.activity.DataflowRunActivityBuilder;
import com.neverwinterdp.scribengin.dataflow.operator.OperatorTaskConfig;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.storage.StorageService;
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
  private TaskService<OperatorTaskConfig> taskService ;
  
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
   
    taskMonitor = new DataflowTaskMonitor();
    taskService = new TaskService<>(dflRegistry.getTaskRegistry());
    taskService.addTaskMonitor(taskMonitor);
  }
  
  public void run() throws Exception {
    System.out.println("DataflowMasterService: run()");
    activityService.queue(new DataflowInitActivityBuilder().build());
    activityService.queue(new DataflowRunActivityBuilder().build());
  }
  
  public void waitForTermination() throws Exception {
    System.out.println("DataflowMasterService: waitForTermination()");
    long maxRunTime = dflRegistry.getConfigRegistry().getDataflowConfig().getWorker().getMaxRunTime();
    if(!taskMonitor.waitForAllTaskFinish(maxRunTime)) {
//      activityService.queue(new DataflowStopActivityBuilder().build());
//      dataflowTaskMonitor.waitForAllTaskFinish(-1);
    }
    workerMonitor.waitForAllWorkerTerminated();
    //finish
    dflRegistry.setStatus(DataflowLifecycleStatus.FINISH);
    System.out.println("DataflowMasterService: waitForTermination() done!!!");
  }
}
