package com.neverwinterdp.scribengin.dataflow.runtime.master;

import org.slf4j.Logger;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.task.dedicated.DedicatedTaskService;
import com.neverwinterdp.registry.task.dedicated.DedicatedTaskWatcherService;
import com.neverwinterdp.registry.txevent.TXEvent;
import com.neverwinterdp.registry.txevent.TXEventBroadcaster;
import com.neverwinterdp.registry.txevent.TXEventNotification;
import com.neverwinterdp.registry.txevent.TXEventWatcher;
import com.neverwinterdp.scribengin.dataflow.DataflowEvent;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.runtime.DataStreamOperatorDescriptor;
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
  
  private DataflowActivityExecutor activityExecutor;
  
  private MTMergerService mtMergerService;
  
  private DataflowWorkerMonitor workerMonitor;
  
  private DataflowTaskMonitor taskMonitor;
  
  private DedicatedTaskService<DataStreamOperatorDescriptor> taskService ;
  private DedicatedTaskWatcherService<DataStreamOperatorDescriptor> taskWatcherService ;
  
  private DataflowMasterEventWatcher eventWatcher ;
  
  private boolean simulateKill = false;
  
  public VMConfig getVMConfig() { return vmDescriptor.getVmConfig(); }
  
  public DataflowRegistry getDataflowRegistry() { return this.dflRegistry; }
  
  public StorageService getStorageService() { return storageService; }

  public DataflowTaskMonitor getDataflowTaskMonitor() { return this.taskMonitor; }
  
  public DataflowWorkerMonitor getDataflowWorkerMonitor() { return this.workerMonitor; }
  
  public DataflowActivityExecutor getDataflowActivityExecutor() {
    return activityExecutor;
  }
  
  @Inject
  public void onInject(Injector container, LoggerFactory lfactory) throws Exception {
    logger = lfactory.getLogger(MasterService.class);
  }
  
  public void addWorker(VMDescriptor vmDescriptor) throws RegistryException {
    workerMonitor.addWorker(vmDescriptor);
  }
  
  public void init() throws Exception {
    logger.info("Start init()");
    dflRegistry.setDataflowStatus(DataflowLifecycleStatus.INIT);
    dflRegistry.initRegistry();
    
    taskService = new DedicatedTaskService<>(dflRegistry.getTaskRegistry(), null);
    taskMonitor = new DataflowTaskMonitor();
    taskWatcherService = new DedicatedTaskWatcherService<>(dflRegistry.getTaskRegistry());
    taskWatcherService.addTaskMonitor(taskMonitor);

    TXEventBroadcaster broadcaster = dflRegistry.getMasterRegistry().getMasterEventBroadcaster();
    String masterEvtPath = broadcaster.getEventPath();
    eventWatcher = new DataflowMasterEventWatcher(dflRegistry, masterEvtPath, vmDescriptor.getVmId());
    
    mtMergerService  = new MTMergerService(dflRegistry);
    activityExecutor = new DataflowActivityExecutor();
    workerMonitor = new DataflowWorkerMonitor(this);
    logger.info("Finish init()");
  }
  
  public void run() throws Exception {
    logger.info("Start run()");
    
    if(dflRegistry.getRegistryStatus() != DataflowRegistry.RegistryStatus.Ready) {
      new DataflowInitActivity(this).execute();
    }
    workerMonitor.onInit();
    
    activityExecutor.add(new DataflowAllocateMasterActivity(this));
    activityExecutor.add(new DataflowRunActivity(this));
  
    mtMergerService.start();
    logger.info("Finish run()");
  }
  
  public void stop() throws Exception {
    activityExecutor.add(new DataflowStopActivity(this));
  }
  
  public void simulateKill() {
    System.err.println("MasterService: start simulateKill()"); 
    logger.info("Start simulateKill()");
    simulateKill = true;
    workerMonitor.simulateKill();
    logger.info("Finish simulateKill()");
    System.err.println("MasterService: finish simulateKill()");
  }
  
  public void waitForTermination() throws Exception {
    System.out.println("MasterService: start taskService.getTaskRegistry().countActiveExecutors() == 0;");
    while(taskService.getTaskRegistry().countActiveExecutors() == 0 && !simulateKill) {
      Thread.sleep(1000);
    }
    
    System.out.println("MasterService: start workerMonitor.waitForAllWorkerTerminated();");
    workerMonitor.waitForAllWorkerTerminated();
    System.out.println("MasterService: finish workerMonitor.waitForAllWorkerTerminated();");
    
    if(simulateKill) {
      mtMergerService.simulateKill();
      throw new Exception("Simulate kill");
    }
    
    System.out.println("MasterService: start taskService.getTaskRegistry().countActiveExecutors() > 0");
    while(taskService.getTaskRegistry().countActiveExecutors() > 0) {
      Thread.sleep(1000);
    }
    
    taskService.onDestroy();
    taskWatcherService.onDestroy();
    System.out.println("MasterService: waitForTermination(), taskService.onDestroy();");
    
    activityExecutor.onDestroy();
    mtMergerService.onDestroy();
    //finish
    dflRegistry.setDataflowStatus(DataflowLifecycleStatus.STOP);
    System.out.println("MasterService: waitForTermination(), done!!!");
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
      } else if(taskEvent == DataflowEvent.SimulateKillMaster) {
        simulateKill();
        logger.info("Dataflow Master detect the stop event!");
      }
      notify(txEvent, TXEventNotification.Status.Complete);
    }
  }
}
