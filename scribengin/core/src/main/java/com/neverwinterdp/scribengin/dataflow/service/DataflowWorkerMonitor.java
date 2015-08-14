package com.neverwinterdp.scribengin.dataflow.service;

import java.util.HashMap;
import java.util.Map;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.event.NodeEventWatcher;
import com.neverwinterdp.registry.notification.Notifier;
import com.neverwinterdp.scribengin.dataflow.activity.AllocateWorkerActivityBuilder;
import com.neverwinterdp.scribengin.dataflow.activity.DataflowActivityService;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.worker.DataflowWorkerStatus;
import com.neverwinterdp.vm.VMDescriptor;

public class DataflowWorkerMonitor {
  private DataflowRegistry dataflowRegistry ;
  private DataflowActivityService activityService ;
  private Map<String, DataflowWorkerHeartbeatListener> workerHeartbeatListeners = new HashMap<>();
  
  public DataflowWorkerMonitor(DataflowRegistry dflRegistry, DataflowActivityService activityService) throws RegistryException {
    this.dataflowRegistry = dflRegistry;
    this.activityService = activityService ;
  }

  synchronized public void addWorker(VMDescriptor vmDescriptor) throws RegistryException {
    DataflowWorkerHeartbeatListener listener = new DataflowWorkerHeartbeatListener (vmDescriptor) ;
    workerHeartbeatListeners.put(vmDescriptor.getRegistryPath(), listener);
  }
  
  synchronized void removeWorkerListener(String heartbeatPath) throws Exception {
    Node heartbeatNode = new Node(dataflowRegistry.getRegistry(), heartbeatPath);
    Node vmNode = heartbeatNode.getParentNode().getParentNode();
    workerHeartbeatListeners.remove(vmNode.getPath());
    dataflowRegistry.getWorkerRegistry().historyWorker(vmNode.getName());
    
    DataflowWorkerStatus dataflowWorkerStatus = 
        dataflowRegistry.getWorkerRegistry().getDataflowWorkerStatus(vmNode.getName());
    if(dataflowWorkerStatus.lessThan(DataflowWorkerStatus.TERMINATED)) {
      DataflowWorkerStatus workerStatus = DataflowWorkerStatus.TERMINATED_WITH_ERROR;
      dataflowRegistry.getWorkerRegistry().setWorkerStatus(vmNode.getName(), workerStatus);
      
      Activity activity = new AllocateWorkerActivityBuilder().build();
      activityService.queue(activity);
      Notifier notifier = dataflowRegistry.getDataflowWorkerNotifier();
      notifier.warn("dataflow-worker-faillure-detection", "Detect a failed dataflow worker " + vmNode.getName() + " and allocate a new one");
    } else {
      notifyWorkerTerminated();
    }
  }
  
  synchronized void notifyWorkerTerminated() {
    notifyAll();
  }
  
  synchronized void waitForAllWorkerTerminated() throws InterruptedException {
    if(workerHeartbeatListeners.size() == 0) return;
    while(workerHeartbeatListeners.size() > 0) {
      wait();
    }
  }
  
  public class DataflowWorkerHeartbeatListener extends NodeEventWatcher {
    public DataflowWorkerHeartbeatListener(VMDescriptor vmDescriptor) throws RegistryException {
      super(dataflowRegistry.getRegistry(), true);
      watchExists(vmDescriptor.getRegistryPath() + "/status/heartbeat");
    }
    
    @Override
    public void processNodeEvent(NodeEvent nodeEvent) throws Exception {
      if(nodeEvent.getType() == NodeEvent.Type.DELETE) {
        setComplete();
        removeWorkerListener(nodeEvent.getPath());
      }
    }
  }
}