package com.neverwinterdp.scribengin.dataflow.worker;

import org.slf4j.Logger;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.neverwinterdp.scribengin.dataflow.master.MasterService;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.storage.StorageService;
import com.neverwinterdp.util.log.LoggerFactory;
import com.neverwinterdp.vm.VMConfig;
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
  
  private TaskExecutorService executorService;
  
  public Logger getLogger() { return this.logger; }
  
  public VMDescriptor getVMDescriptor() { return this.vmDescriptor; }
  
  public DataflowRegistry getDataflowRegistry() { return this.dflRegistry; }
  
  public StorageService getStorageService() { return storageService; }

  public MetricRegistry getMetricRegistry() { return this.metricRegistry; }
  
  @Inject
  public void onInject(Injector container, LoggerFactory lfactory) throws Exception {
    logger = lfactory.getLogger(MasterService.class);
  }
  
  public void init() throws Exception {
    System.out.println("DataflowWorkerService: init()");
    executorService = new TaskExecutorService(dflRegistry);
  }
  
  public void run() throws Exception {
    System.out.println("DataflowMasterService: run()");
    executorService.start();
  }
  
  public void waitForTermination() {
    System.out.println("DataflowWorkerService: waitForTermination()");
    try {
      executorService.waitForTermination();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
  
  public void shutdown() {
  }
  
  public void simulateKill() {
  }
}
