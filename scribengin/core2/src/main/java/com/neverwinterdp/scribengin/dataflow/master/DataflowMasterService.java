package com.neverwinterdp.scribengin.dataflow.master;

import org.slf4j.Logger;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.master.activity.DataflowInitActivityBuilder;
import com.neverwinterdp.scribengin.dataflow.master.activity.DataflowMasterActivityService;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.storage.StorageService;
import com.neverwinterdp.util.log.LoggerFactory;
import com.neverwinterdp.vm.VMConfig;

@Singleton
public class DataflowMasterService {
  private Logger logger ;
  
  @Inject
  private VMConfig vmConfig;
 
  @Inject
  private DataflowRegistry dflRegistry;
  
  @Inject
  private StorageService storageService ;
  
  
  private DataflowMasterActivityService activityService;

  public DataflowRegistry getDataflowRegistry() { return this.dflRegistry; }
  
  public StorageService getStorageService() { return storageService; }

  @Inject
  public void onInject(Injector container, LoggerFactory lfactory) throws Exception {
    logger = lfactory.getLogger(DataflowMasterService.class);
    activityService = new DataflowMasterActivityService(container, dflRegistry) ;
  }
  
  public void init() throws Exception {
    System.out.println("DataflowMasterService: init()");
    dflRegistry.setStatus(DataflowLifecycleStatus.INIT);
    dflRegistry.initRegistry();
    activityService.queue(new DataflowInitActivityBuilder().build());
  }
  
  public void run() {
    System.out.println("DataflowMasterService: run()");
  }
  
  public void waitForTermination() {
    System.out.println("DataflowMasterService: waitForTermination()");
    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
