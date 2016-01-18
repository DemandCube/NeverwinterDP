package com.neverwinterdp.scribengin.dataflow.runtime.worker;

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
      TaskExecutorDescriptor executor = new TaskExecutorDescriptor(vmDescriptor.getVmId() + "-executor-" + i, vmDescriptor.getVmId());
      taskService.addExecutor(executor, 2);
    }
    Node workerNode = dflRegistry.getWorkerRegistry().getWorkerNode(vmDescriptor.getVmId());
    notifier = new Notifier(dflRegistry.getRegistry(), workerNode.getPath() + "/notifications", "dataflow-worker-service");
    notifier.initRegistry();
    
    TXEventBroadcaster broadcaster = dflRegistry.getWorkerRegistry().getWorkerEventBroadcaster();
    String workerEvtPath = broadcaster.getEventPath();
    dataflowWorkerEventWatcher = new DataflowWorkerEventWatcher(dflRegistry, workerEvtPath, vmDescriptor.getVmId());
  }
  
  public Logger getLogger() { return logger; }
  
  public VMDescriptor getVMDescriptor() { return vmDescriptor; }
  
  public DataflowRegistry getDataflowRegistry() { return dflRegistry; }
  
  public StorageService getStorageService() { return storageService; }

  public MetricRegistry getMetricRegistry() { return metricRegistry; }
  
  public Injector getServiceContainer() { return this.serviceContainer; }
  
  public boolean isSimulateKill() { return simulateKill; }
  
  public void run() throws Exception {
    System.out.println("DataflowWorkerService: run()");
    workerStatus = DataflowWorkerStatus.RUNNING;
    dflRegistry.getWorkerRegistry().setWorkerStatus(vmDescriptor, workerStatus);
    taskService.getTaskExecutorService().startExecutors(3000);
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
    dflRegistry.getWorkerRegistry().saveMetric(vmDescriptor.getVmId(), metricRegistry);
  }
  
  
  public void waitForTermination() throws RegistryException, InterruptedException {
    System.out.println("DataflowWorkerService: Start waitForTermination()");
    try {
      taskService.getTaskExecutorService().awaitTermination();
    } catch(InterruptedException ex) {
      if(simulateKill) {
        System.err.println("DataflowWorkerService: Finish waitForTermination() with Simulate Kill");
        dataflowWorkerEventWatcher.setComplete();
        throw new RuntimeException("Simulate Kill", ex) ;
      }
      throw ex;
    }
    dataflowWorkerEventWatcher.setComplete();
    
    taskService.onDestroy();
    workerStatus = DataflowWorkerStatus.TERMINATED;
    dflRegistry.getWorkerRegistry().setWorkerStatus(vmDescriptor, workerStatus);
    dflRegistry.getWorkerRegistry().saveMetric(vmDescriptor.getVmId(), metricRegistry);
    System.out.println("DataflowWorkerService: Finisht waitForTermination()");
  }
  
  public void simulateKill() throws Exception {
    System.err.println("WorkerService: start simulateKill()"); 
    logger.info("Start simulateKill()");
    simulateKill = true ;
    if(workerStatus.lessThan(DataflowWorkerStatus.TERMINATED)) {
      System.err.println("WorkerService: taskService.getTaskExecutorService().simulateKill()"); 
      taskService.simulateKill();
      dflRegistry.getRegistry().shutdown();
    }
    System.err.println("WorkerService: finish simulateKill()"); 
    logger.info("Finish simulateKill()");
  }
  
  public class DataflowWorkerEventWatcher extends TXEventWatcher {
    public DataflowWorkerEventWatcher(DataflowRegistry dflRegistry, String eventsPath, String clientId) throws RegistryException {
      super(dflRegistry.getRegistry(), eventsPath, clientId);
    }
    
    public void onTXEvent(TXEvent txEvent) throws Exception {
      DataflowWorkerEvent taskEvent = txEvent.getDataAs(DataflowWorkerEvent.class);
      if(taskEvent == DataflowWorkerEvent.StopInput) {
        logger.info("Dataflow worker detect stop input event!");
        stopInput();
      } else if(taskEvent == DataflowWorkerEvent.StopWorker) {
        logger.info("Dataflow worker detect stop worker event. worker = " + vmDescriptor.getVmId());
        System.err.println("Dataflow worker detect stop worker event. worker = " + vmDescriptor.getVmId());
        shutdown() ;
      }
      notify(txEvent, TXEventNotification.Status.Complete);
    }
  }

}
