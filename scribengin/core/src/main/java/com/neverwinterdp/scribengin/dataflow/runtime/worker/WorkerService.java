package com.neverwinterdp.scribengin.dataflow.runtime.worker;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.notification.Notifier;
import com.neverwinterdp.registry.task.TaskExecutorDescriptor;
import com.neverwinterdp.registry.task.dedicated.DedicatedTaskContext;
import com.neverwinterdp.registry.task.dedicated.DedicatedTaskService;
import com.neverwinterdp.registry.task.dedicated.TaskExecutorEvent;
import com.neverwinterdp.registry.task.dedicated.TaskSlotExecutor;
import com.neverwinterdp.registry.task.dedicated.TaskSlotExecutorFactory;
import com.neverwinterdp.registry.txevent.TXEvent;
import com.neverwinterdp.registry.txevent.TXEventBroadcaster;
import com.neverwinterdp.registry.txevent.TXEventNotification;
import com.neverwinterdp.registry.txevent.TXEventWatcher;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowEvent;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.runtime.DataStreamOperatorDescriptor;
import com.neverwinterdp.scribengin.dataflow.runtime.DataStreamOperatorTaskSlotExecutor;
import com.neverwinterdp.storage.StorageService;
import com.neverwinterdp.util.log.LoggerFactory;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.yara.MetricRegistry;

public class WorkerService {
  private Logger logger ;
  
  @Inject
  private VMDescriptor vmDescriptor;
 
  @Inject
  private DataflowRegistry dflRegistry;
  
  @Inject
  private StorageService storageService ;
  
  @Inject
  private MetricRegistry   metricRegistry ;
  
  private Injector         serviceContainer;
  
  private DedicatedTaskService<DataStreamOperatorDescriptor> taskService;
  
  private DataflowWorkerEventWatcher   dataflowWorkerEventWatcher ;

  private DataflowWorkerStatus workerStatus = DataflowWorkerStatus.INIT;
  
  private Notifier         notifier ;
  private boolean simulateKill = false ;
  
  @Inject
  public void onInject(Injector container, LoggerFactory lfactory) throws Exception {
    logger = lfactory.getLogger(WorkerService.class);
    serviceContainer = container;
  }
  
  public void init() throws Exception {
    System.out.println("DataflowWorkerService: init()");
    workerStatus = DataflowWorkerStatus.INIT;
    dflRegistry.getWorkerRegistry().setWorkerStatus(vmDescriptor, workerStatus);
    
    TaskSlotExecutorFactory<DataStreamOperatorDescriptor> taskSlotExecutorFactory = new TaskSlotExecutorFactory<DataStreamOperatorDescriptor>() {
      @Override
      public TaskSlotExecutor<DataStreamOperatorDescriptor> create(DedicatedTaskContext<DataStreamOperatorDescriptor> context) throws Exception {
        return new  DataStreamOperatorTaskSlotExecutor(WorkerService.this, context);
      }
    };
    
    DataflowDescriptor dflConfig = dflRegistry.getConfigRegistry().getDataflowDescriptor();
    taskService = new DedicatedTaskService<DataStreamOperatorDescriptor>(dflRegistry.getTaskRegistry(), taskSlotExecutorFactory);
    for(int i = 0; i < dflConfig.getWorker().getNumOfExecutor(); i++) {
      TaskExecutorDescriptor executor = new TaskExecutorDescriptor(vmDescriptor.getId() + "-executor-" + i, vmDescriptor.getId());
      taskService.addExecutor(executor, 2);
    }
    Node workerNode = dflRegistry.getWorkerRegistry().getWorkerNode(vmDescriptor.getId());
    notifier = new Notifier(dflRegistry.getRegistry(), workerNode.getPath() + "/notifications", "dataflow-worker-service");
    notifier.initRegistry();
    
    TXEventBroadcaster broadcaster = dflRegistry.getWorkerRegistry().getWorkerEventBroadcaster();
    String workerEvtPath = broadcaster.getEventPath();
    dataflowWorkerEventWatcher = new DataflowWorkerEventWatcher(dflRegistry, workerEvtPath, vmDescriptor.getId());
  }
  
  public Logger getLogger() { return logger; }
  
  public VMDescriptor getVMDescriptor() { return vmDescriptor; }
  
  public DataflowRegistry getDataflowRegistry() { return dflRegistry; }
  
  public StorageService getStorageService() { return storageService; }

  public MetricRegistry getMetricRegistry() { return metricRegistry; }
  
  public Injector getServiceContainer() { return this.serviceContainer; }
  
  public void run() throws Exception {
    System.out.println("DataflowMasterService: run()");
    workerStatus = DataflowWorkerStatus.RUNNING;
    dflRegistry.getWorkerRegistry().setWorkerStatus(vmDescriptor, workerStatus);
    taskService.getTaskExecutorService().startExecutors(3000);
  }
  
  public void waitForTermination() throws RegistryException, InterruptedException {
    System.out.println("DataflowWorkerService: waitForTermination()");
    long maxRunTime = dflRegistry.getConfigRegistry().getDataflowDescriptor().getMaxRunTime();
    try {
      taskService.getTaskExecutorService().awaitTermination(maxRunTime, TimeUnit.MILLISECONDS);
    } catch(InterruptedException ex) {
      if(simulateKill) {
        dataflowWorkerEventWatcher.setComplete();
        throw new RuntimeException("Simulate Kill", ex) ;
      }
      throw ex;
    }
    taskService.onDestroy();
    dataflowWorkerEventWatcher.setComplete();
    workerStatus = DataflowWorkerStatus.TERMINATED;
    dflRegistry.getWorkerRegistry().setWorkerStatus(vmDescriptor, workerStatus);
    dflRegistry.getWorkerRegistry().saveMetric(vmDescriptor.getId(), metricRegistry);
  }
  
  public void stopInput() throws Exception {
    TaskExecutorEvent event = new TaskExecutorEvent("StopInput");
    taskService.getTaskExecutorService().broadcast(event);;
  }
  
  public void shutdown() throws InterruptedException, RegistryException {
    taskService.onDestroy();
    dataflowWorkerEventWatcher.setComplete();
    workerStatus = DataflowWorkerStatus.TERMINATED_WITH_INTERRUPT;
    dflRegistry.getWorkerRegistry().setWorkerStatus(vmDescriptor, workerStatus);
    dflRegistry.getWorkerRegistry().saveMetric(vmDescriptor.getId(), metricRegistry);
  }
  
  public void simulateKill() throws Exception {
    System.err.println("WorkerService: simulateKill()"); 
    logger.info("Start kill()");
    notifier.info("start-simulate-kill", "DataflowTaskExecutorService: start simulateKill()");
    simulateKill = true ;
    if(workerStatus.lessThan(DataflowWorkerStatus.TERMINATED)) {
      System.err.println("WorkerService: taskService.getTaskExecutorService().simulateKill()"); 
      taskService.simulateKill();
    }
    notifier.info("finish-simulate-kill", "DataflowTaskExecutorService: finish simulateKill()");
    logger.info("Finish kill()");
  }
  
  public class DataflowWorkerEventWatcher extends TXEventWatcher {
    public DataflowWorkerEventWatcher(DataflowRegistry dflRegistry, String eventsPath, String clientId) throws RegistryException {
      super(dflRegistry.getRegistry(), eventsPath, clientId);
    }
    
    public void onTXEvent(TXEvent txEvent) throws Exception {
      DataflowEvent taskEvent = txEvent.getDataAs(DataflowEvent.class);
      if(taskEvent == DataflowEvent.Pause) {
        logger.info("Dataflow worker detect pause event!");
      } else if(taskEvent == DataflowEvent.StopInput) {
        logger.info("Dataflow worker detect stop input event!");
      } else if(taskEvent == DataflowEvent.StopWorker) {
        logger.info("Dataflow worker detect stop worker event!");
        shutdown() ;
      } else if(taskEvent == DataflowEvent.Resume) {
        logger.info("Dataflow worker detect resume event!");
      }
      notify(txEvent, TXEventNotification.Status.Complete);
    }
  }

}
