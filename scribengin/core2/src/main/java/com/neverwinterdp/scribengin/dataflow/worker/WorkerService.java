package com.neverwinterdp.scribengin.dataflow.worker;

import org.slf4j.Logger;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.notification.Notifier;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.storage.StorageService;
import com.neverwinterdp.util.log.LoggerFactory;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.yara.MetricPrinter;
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
  
  private TaskExecutors taskExecutors;
  
  private DataflowWorkerStatus workerStatus = DataflowWorkerStatus.INIT;
  
  private Notifier         notifier ;
  private boolean kill = false ;
  
  public Logger getLogger() { return logger; }
  
  public VMDescriptor getVMDescriptor() { return vmDescriptor; }
  
  public DataflowRegistry getDataflowRegistry() { return dflRegistry; }
  
  public StorageService getStorageService() { return storageService; }

  public MetricRegistry getMetricRegistry() { return metricRegistry; }
  
  @Inject
  public void onInject(Injector container, LoggerFactory lfactory) throws Exception {
    logger = lfactory.getLogger(WorkerService.class);
  }
  
  public void init() throws Exception {
    System.out.println("DataflowWorkerService: init()");
    workerStatus = DataflowWorkerStatus.INIT;
    dflRegistry.getWorkerRegistry().setWorkerStatus(vmDescriptor, workerStatus);
    taskExecutors = new TaskExecutors(this);
    Node workerNode = dflRegistry.getWorkerRegistry().getWorkerNode(vmDescriptor.getId());
    notifier = new Notifier(dflRegistry.getRegistry(), workerNode.getPath() + "/notifications", "dataflow-worker-service");
    notifier.initRegistry();
  }
  
  public void run() throws Exception {
    System.out.println("DataflowMasterService: run()");
    workerStatus = DataflowWorkerStatus.RUNNING;
    dflRegistry.getWorkerRegistry().setWorkerStatus(vmDescriptor, workerStatus);
    taskExecutors.start();
  }
  
  public void waitForTermination() throws RegistryException, InterruptedException {
    System.out.println("DataflowWorkerService: waitForTermination()");
    taskExecutors.waitForTermination();
    if(taskExecutors.getSimulateKill()) return;
    
    workerStatus = DataflowWorkerStatus.TERMINATED;
    dflRegistry.getWorkerRegistry().setWorkerStatus(vmDescriptor, workerStatus);
    dflRegistry.getWorkerRegistry().saveMetric(vmDescriptor.getId(), metricRegistry);
  }
  
  public void shutdown() {
  }
  
  public void simulateKill() throws Exception {
    logger.info("Start kill()");
    notifier.info("start-simulate-kill", "DataflowTaskExecutorService: start simulateKill()");
    kill = true ;
    if(workerStatus.lessThan(DataflowWorkerStatus.TERMINATED)) {
      taskExecutors.simulateKill();
    }
    notifier.info("finish-simulate-kill", "DataflowTaskExecutorService: finish simulateKill()");
    logger.info("Finish kill()");
  }
}
