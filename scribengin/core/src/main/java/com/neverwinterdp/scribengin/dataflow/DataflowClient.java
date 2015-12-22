package com.neverwinterdp.scribengin.dataflow;

import java.util.List;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.vm.VMDescriptor;

public class DataflowClient {
  private ScribenginClient scribenginClient ;
  private DataflowRegistry dflRegistry ;
  
  public DataflowClient(ScribenginClient scribenginClient, String dataflowPath) throws Exception {
    this.scribenginClient = scribenginClient;
    dflRegistry = new DataflowRegistry(scribenginClient.getRegistry(), dataflowPath) ;
  }
  
  public Registry getRegistry() { return scribenginClient.getRegistry(); }
  
  public DataflowRegistry getDataflowRegistry() { return this.dflRegistry ; }
  
  public ScribenginClient getScribenginClient() { return this.scribenginClient; }
  
  
  public List<VMDescriptor> getActiveDataflowWorkers() throws RegistryException {
    return dflRegistry.getWorkerRegistry().getActiveWorkers();
  }
  
  public int countActiveDataflowWorkers() throws RegistryException {
    return dflRegistry.getWorkerRegistry().countActiveDataflowWorkers();
  }
  
  public DataflowLifecycleStatus getStatus() throws RegistryException {
    return dflRegistry.getStatus() ;
  }
  
  public void waitForEqualOrGreaterThanStatus(long checkPeriod, long timeout, DataflowLifecycleStatus status) throws Exception {
    long stopTime = System.currentTimeMillis() + timeout;
    while(System.currentTimeMillis() < stopTime) {
      DataflowLifecycleStatus currentStatus = dflRegistry.getStatus();
      if(currentStatus.equalOrGreaterThan(status)) return;
      Thread.sleep(checkPeriod);
    }
    String dataflowId = dflRegistry.getConfigRegistry().getDataflowDescriptor().getId();
    throw new Exception("Cannot get the equal or greater than " + status + " after " + timeout + "ms for the dataflow " + dataflowId);
  }
}