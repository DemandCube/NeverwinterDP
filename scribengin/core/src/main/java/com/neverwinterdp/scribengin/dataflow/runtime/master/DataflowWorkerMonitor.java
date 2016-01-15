package com.neverwinterdp.scribengin.dataflow.runtime.master;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.event.NodeEventWatcher;
import com.neverwinterdp.registry.notification.Notifier;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.runtime.worker.DataflowWorkerStatus;
import com.neverwinterdp.vm.VMDescriptor;

public class DataflowWorkerMonitor {
  private MasterService service;
  private Map<String, DataflowWorkerHeartbeatListener> workerHeartbeatListeners = new HashMap<>();
  private boolean simulateKill = false;
  
  public DataflowWorkerMonitor(MasterService service) throws RegistryException {
    this.service = service;
  }

  public void onInit() throws RegistryException {
    List<VMDescriptor> vmDescriptors = service.getDataflowRegistry().getWorkerRegistry().getActiveWorkers();
    for(int i = 0; i < vmDescriptors.size(); i++) {
      addWorker(vmDescriptors.get(i));
    }
  }
  
  synchronized public void simulateKill() {
    simulateKill = true;
    notifyAll();
  }
  
  synchronized public void addWorker(VMDescriptor vmDescriptor) throws RegistryException {
    DataflowWorkerHeartbeatListener listener = new DataflowWorkerHeartbeatListener (vmDescriptor) ;
    workerHeartbeatListeners.put(vmDescriptor.getRegistryPath(), listener);
  }
  
  synchronized void onWorkerBrokenHeartbeat(String heartbeatPath) throws Exception {
    System.out.println("DataflowWorkerMonitor: onWorkerBrokenHeartbeat " + heartbeatPath);
    DataflowRegistry dflRegistry = service.getDataflowRegistry();
    Node heartbeatNode = new Node(dflRegistry.getRegistry(), heartbeatPath);
    Node vmNode = heartbeatNode.getParentNode().getParentNode();
    workerHeartbeatListeners.remove(vmNode.getPath());
    dflRegistry.getWorkerRegistry().historyWorker(vmNode.getName());
    
    DataflowWorkerStatus dataflowWorkerStatus = dflRegistry.getWorkerRegistry().getDataflowWorkerStatus(vmNode.getName());
    if(dataflowWorkerStatus.lessThan(DataflowWorkerStatus.TERMINATED)) {
      DataflowWorkerStatus workerStatus = DataflowWorkerStatus.TERMINATED_WITH_ERROR;
      dflRegistry.getWorkerRegistry().setWorkerStatus(vmNode.getName(), workerStatus);
      
      service.getDataflowActivityExecutor().add(new AllocateWorkerActivity(service));
      Notifier notifier = dflRegistry.getDataflowWorkerNotifier();
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
    while(workerHeartbeatListeners.size() > 0 && !simulateKill) {
      wait();
    }
  }
  
  public class DataflowWorkerHeartbeatListener extends NodeEventWatcher {
    public DataflowWorkerHeartbeatListener(VMDescriptor vmDescriptor) throws RegistryException {
      super(service.getDataflowRegistry().getRegistry(), true);
      watchExists(vmDescriptor.getRegistryPath() + "/status/heartbeat");
    }
    
    @Override
    public void processNodeEvent(NodeEvent nodeEvent) throws Exception {
      if(nodeEvent.getType() == NodeEvent.Type.DELETE) {
        setComplete();
        onWorkerBrokenHeartbeat(nodeEvent.getPath());
      }
    }
  }
}