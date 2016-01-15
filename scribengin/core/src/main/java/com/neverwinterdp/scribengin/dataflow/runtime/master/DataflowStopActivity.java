package com.neverwinterdp.scribengin.dataflow.runtime.master;

import java.util.List;

import com.neverwinterdp.message.TrackingWindowRegistry;
import com.neverwinterdp.message.TrackingWindowReport;
import com.neverwinterdp.registry.txevent.TXEvent;
import com.neverwinterdp.registry.txevent.TXEventBroadcaster;
import com.neverwinterdp.registry.txevent.TXEventNotificationCompleteListener;
import com.neverwinterdp.registry.txevent.TXEventNotificationWatcher;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.runtime.worker.DataflowWorkerEvent;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;

public class DataflowStopActivity implements DataflowMasterActivity {
  private MasterService service;
  
  public DataflowStopActivity(MasterService service) {
    this.service = service;
  }
  
  public void execute() throws Exception {
    stopInput();
    stopWorkers();
    stopSlaveMasters();
    setStopStatus();
  }
  
  public void stopInput() throws Exception {
    DataflowRegistry dflRegistry = service.getDataflowRegistry();
    if(DataflowLifecycleStatus.RUNNING != dflRegistry.getDataflowStatus()) {
      return ;
    }
    
    System.err.println("BroadcastStopInputStepExecutor: start broadcast StopInput");
    List<String> workers = dflRegistry.getWorkerRegistry().getActiveWorkerIds() ;
    TXEvent pEvent = new TXEvent("stop-input", DataflowWorkerEvent.StopInput);
    TXEventBroadcaster broadcaster = dflRegistry.getWorkerRegistry().getWorkerEventBroadcaster();
    TXEventNotificationWatcher watcher = broadcaster.broadcast(pEvent, new TXEventNotificationCompleteListener());
    int countNotification = watcher.waitForNotifications(workers.size(), 60 * 1000);
    if(countNotification != workers.size()) {
      throw new Exception("Expect " + workers.size() + ", but only get " + countNotification) ;
    }
    watcher.complete();
    System.err.println("BroadcastStopInputStepExecutor: complete broadcast StopInput, num of worker = " + workers.size());
    
    TrackingWindowRegistry mtRegistry = dflRegistry.getMessageTrackingRegistry();
    int checkCount = 0;
    boolean noMessageLeft = false;
    while(checkCount < 120 && !noMessageLeft) {
      Thread.sleep(500);
      TrackingWindowReport trackingReport  = mtRegistry.getReport();
      int commitWindowLefts = mtRegistry.getProgressCommitWindowIds().size();
      System.err.println("BroadcastStopInputStepExecutor: tracking count = " + trackingReport.getTrackingCount() + ", commitWindowLefts = " + commitWindowLefts);
      if(commitWindowLefts == 0) noMessageLeft = true;
      checkCount++;
    }
    if(!noMessageLeft) {
      throw new Exception("Expect no message in the scribengin") ;
    }
  }
  
  public void stopWorkers() throws Exception {
    DataflowRegistry dflRegistry = service.getDataflowRegistry();
    if(DataflowLifecycleStatus.RUNNING != dflRegistry.getDataflowStatus()) {
      return ;
    }
    
    List<String> workers = dflRegistry.getWorkerRegistry().getActiveWorkerIds() ;
    TXEvent pEvent = new TXEvent("stop", DataflowWorkerEvent.StopWorker);
    TXEventBroadcaster broadcaster = dflRegistry.getWorkerRegistry().getWorkerEventBroadcaster();
    TXEventNotificationWatcher watcher = broadcaster.broadcast(pEvent, new TXEventNotificationCompleteListener());
    int countNotification = watcher.waitForNotifications(workers.size(), 60 * 1000);
    if(countNotification != workers.size()) {
      throw new Exception("Expect " + workers.size() + ", but only get " + countNotification) ;
    }
    watcher.complete();
  }
  
  public void stopSlaveMasters() throws Exception {
    DataflowRegistry dflRegistry = service.getDataflowRegistry();
    List<VMDescriptor> vmDescriptors = dflRegistry.getMasterRegistry().getMasterVMDescriptors();
    VMConfig currentVMMaster = service.getVMConfig();
    VMClient vmClient = new VMClient(dflRegistry.getRegistry());
    for(int i = 0; i < vmDescriptors.size(); i++) {
      VMDescriptor vmDescriptor = vmDescriptors.get(i);
      if(!currentVMMaster.getVmId().equals(vmDescriptor.getId())) {
        System.err.println("DataflowStopActivity: stop master " + vmDescriptor.getId());
        vmClient.shutdown(vmDescriptor);
      }
    }
  }
  
  public void setStopStatus() throws Exception {
    DataflowRegistry dflRegistry = service.getDataflowRegistry();
    dflRegistry.setDataflowStatus(DataflowLifecycleStatus.STOP);
    System.err.println("DataflowService Stop Activity set STOP status done!!!") ;
  }
}
