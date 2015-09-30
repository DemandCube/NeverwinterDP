package com.neverwinterdp.scribengin.dataflow.master;

import org.slf4j.Logger;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.util.log.LoggerFactory;
import com.neverwinterdp.vm.VMConfig;

public class DataflowMasterService {
  private Logger logger ;
  
  @Inject
  private VMConfig vmConfig;
 
  @Inject
  private DataflowRegistry dflRegistry;
  
  @Inject
  public void onInject(Injector container, LoggerFactory lfactory) throws Exception {
    logger = lfactory.getLogger(DataflowMasterService.class);
  }
  
  public void init() throws RegistryException {
    System.out.println("DataflowMasterService: init()");
    dflRegistry.initRegistry();
  }
  
  public void run() {
    System.out.println("DataflowMasterService: run()");
  }
  
  public void waitForTermination() {
    System.out.println("DataflowMasterService: waitForTermination()");
  }
}
