package com.neverwinterdp.scribengin.dataflow.runtime.master;

import org.slf4j.Logger;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.task.dedicated.DedicatedTaskService;
import com.neverwinterdp.registry.txevent.TXEvent;
import com.neverwinterdp.registry.txevent.TXEventBroadcaster;
import com.neverwinterdp.registry.txevent.TXEventNotification;
import com.neverwinterdp.registry.txevent.TXEventWatcher;
import com.neverwinterdp.scribengin.dataflow.DataflowEvent;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.runtime.DataStreamOperatorDescriptor;
import com.neverwinterdp.scribengin.dataflow.runtime.master.activity.DataflowInitActivityBuilder;
import com.neverwinterdp.scribengin.dataflow.runtime.master.activity.DataflowMasterActivityService;
import com.neverwinterdp.scribengin.dataflow.runtime.master.activity.DataflowRunActivityBuilder;
import com.neverwinterdp.scribengin.dataflow.runtime.master.activity.DataflowStopActivityBuilder;
import com.neverwinterdp.scribengin.dataflow.runtime.worker.WorkerService.DataflowWorkerEventWatcher;
import com.neverwinterdp.storage.StorageService;
import com.neverwinterdp.util.log.LoggerFactory;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;

@Singleton
public class MasterService {
  private Logger logger ;
  
  @Inject
  private VMDescriptor vmDescriptor;
  
  @Inject
  private DataflowRegistry dflRegistry;
  
  @Inject
  private StorageService storageService ;
  
  private MTMergerService mtMergerService;
  
  private DataflowWorkerMonitor workerMonitor;
  
  private DataflowTaskMonitor  taskMonitor;
  private DedicatedTaskService<DataStreamOperatorDescriptor> taskService ;
  
  private DataflowMasterEventWatcher   dataflowMasterEventWatcher ;
  
  public VMConfig getVMConfig() { return vmDescriptor.getVmConfig(); }
  
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
    logger.info("Start init()");
    dflRegistry.setStatus(DataflowLifecycleStatus.INIT);
    dflRegistry.initRegistry();
    
    workerMonitor = new DataflowWorkerMonitor(dflRegistry, activityService);
    
    taskService = new DedicatedTaskService<>(dflRegistry.getTaskRegistry(), null);
    taskMonitor = new DataflowTaskMonitor();
    taskService.addTaskMonitor(taskMonitor);

    TXEventBroadcaster broadcaster = dflRegistry.getMasterRegistry().getMaserEventBroadcaster();
    String masterEvtPath = broadcaster.getEventPath();
    dataflowMasterEventWatcher = new DataflowMasterEventWatcher(dflRegistry, masterEvtPath, vmDescriptor.getId());
    
    mtMergerService = new MTMergerService(dflRegistry);
    
    logger.info("Finish init()");
  }
  
  public void run() throws Exception {
    logger.info("Start run()");
    activityService.queue(new DataflowInitActivityBuilder().build());
    activityService.queue(new DataflowRunActivityBuilder().build());
    logger.info("Finish run()");
  }
  
  public void stop() throws Exception {
    activityService.queue(new DataflowStopActivityBuilder().build());
  }
  
  public void waitForTermination() throws Exception {
    //long maxRunTime = dflRegistry.getConfigRegistry().getDataflowDescriptor().getMaxRunTime();
    //System.out.println("DataflowMasterService: waitForTermination()");
    
    System.out.println("DataflowMasterService: taskMonitor.waitForAllTaskFinish(30000);");
    taskMonitor.waitForAllTaskFinish(30000);
    System.out.println("DataflowMasterService: start workerMonitor.waitForAllWorkerTerminated();");
    workerMonitor.waitForAllWorkerTerminated();
    System.out.println("DataflowMasterService: finish workerMonitor.waitForAllWorkerTerminated();");
    
    //System.out.println("DataflowMasterService: taskMonitor.waitForAllTaskFinish(maxRunTime);");
    //taskMonitor.waitForAllTaskFinish(maxRunTime);
    System.out.println("DataflowMasterService: waitForTermination(), wait for all task terminate");
    
    taskService.onDestroy();
    System.out.println("DataflowMasterService: waitForTermination(), taskService.onDestroy();");
    activityService.onDestroy();
    System.out.println("DataflowMasterService: waitForTermination(), activityService.onDestroy()");
    
    mtMergerService.onDestroy();
    //finish
    dflRegistry.setStatus(DataflowLifecycleStatus.FINISH);
    System.out.println("DataflowMasterService: waitForTermination(), done!!!");
  }
  
  public class DataflowMasterEventWatcher extends TXEventWatcher {
    public DataflowMasterEventWatcher(DataflowRegistry dflRegistry, String eventsPath, String clientId) throws RegistryException {
      super(dflRegistry.getRegistry(), eventsPath, clientId);
    }
    
    public void onTXEvent(TXEvent txEvent) throws Exception {
      DataflowEvent taskEvent = txEvent.getDataAs(DataflowEvent.class);
      if(taskEvent == DataflowEvent.Stop) {
        stop();
        logger.info("Dataflow Master detect the stop event!");
      }
      notify(txEvent, TXEventNotification.Status.Complete);
    }
  }
}
