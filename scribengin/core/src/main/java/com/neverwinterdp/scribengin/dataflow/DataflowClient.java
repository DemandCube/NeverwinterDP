package com.neverwinterdp.scribengin.dataflow;

import java.util.List;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.txevent.TXEvent;
import com.neverwinterdp.registry.txevent.TXEventNotificationWatcher;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.event.DataflowEvent;
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
  
  
  public VMDescriptor getDataflowMaster() throws RegistryException { 
    return dflRegistry.getDataflowMaster() ;
  }
  
  public List<VMDescriptor> getActiveDataflowWorkers() throws RegistryException {
    return dflRegistry.getWorkerRegistry().getActiveWorkers();
  }
  
  public int countActiveDataflowWorkers() throws RegistryException {
    return dflRegistry.getWorkerRegistry().countActiveDataflowWorkers();
  }
  
  public void setDataflowEvent(DataflowEvent event) throws RegistryException {
    dflRegistry.broadcastMasterEvent(event);
  }
  
  public TXEventNotificationWatcher broadcastDataflowEvent(DataflowEvent event) throws Exception {
    String eventName = event.toString().toLowerCase();
    TXEvent txEvent = new TXEvent(eventName, event) ;
    return dflRegistry.getMasterEventBroadcaster().broadcast(txEvent);
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
    throw new Exception("Cannot get the equal or greater than " + status + " after " + timeout + "ms");
  }
}