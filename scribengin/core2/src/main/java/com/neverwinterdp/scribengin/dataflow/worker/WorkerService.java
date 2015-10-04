package com.neverwinterdp.scribengin.dataflow.worker;

import org.slf4j.Logger;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.storage.StorageService;
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
  
  private TaskExecutors taskExecutors;
  
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
    taskExecutors = new TaskExecutors(this);
  }
  
  public void run() throws Exception {
    System.out.println("DataflowMasterService: run()");
    taskExecutors.start();
  }
  
  public void waitForTermination() {
    System.out.println("DataflowWorkerService: waitForTermination()");
    try {
      taskExecutors.waitForTermination();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
  
  public void shutdown() {
  }
  
  public void simulateKill() {
  }
}
