package com.neverwinterdp.scribengin.dataflow.registry;

import java.util.List;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.RefNode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.Transaction;
import com.neverwinterdp.registry.txevent.TXEventBroadcaster;
import com.neverwinterdp.scribengin.dataflow.worker.DataflowTaskExecutorDescriptor;
import com.neverwinterdp.scribengin.dataflow.worker.DataflowWorkerStatus;
import com.neverwinterdp.vm.VMDescriptor;

public class DataflowWorkerRegistry {
  final static public String ALL_WORKERS_PATH       = "workers/all";
  final static public String ACTIVE_WORKERS_PATH    = "workers/active";
  final static public String HISTORY_WORKERS_PATH   = "workers/history";
  final static public String WORKER_EVENT_PATH      = "workers/events" ;

  private Registry           registry;
  private String             dataflowPath;
  private Node               allWorkers;
  private Node               activeWorkers;
  private Node               historyWorkers;
  private TXEventBroadcaster workerEventBroadcaster;
  
  public DataflowWorkerRegistry(Registry registry, String dataflowPath) throws RegistryException {
    this.registry = registry ;
    this.dataflowPath = dataflowPath ;
    
    workerEventBroadcaster = 
      new TXEventBroadcaster(registry, dataflowPath + "/" + WORKER_EVENT_PATH, false);
    allWorkers = registry.get(dataflowPath + "/" + ALL_WORKERS_PATH);
    activeWorkers = registry.get(dataflowPath + "/" + ACTIVE_WORKERS_PATH);
    historyWorkers = registry.get(dataflowPath + "/" + HISTORY_WORKERS_PATH);
  }
  
  public void initRegistry() throws Exception {
    allWorkers.createIfNotExists();
    activeWorkers.createIfNotExists();
    historyWorkers.createIfNotExists();
    registry.createIfNotExist(dataflowPath + "/" + WORKER_EVENT_PATH);
  }

  public Node getWorkerNode(String vmId) throws RegistryException { 
    return allWorkers.getChild(vmId) ; 
  }
  
  public DataflowWorkerStatus[] getActiveWorkerStatus() throws RegistryException {
    List<String> vmIds = activeWorkers.getChildren();
    DataflowWorkerStatus[] status = new DataflowWorkerStatus[vmIds.size()];
    for(int i = 0; i < vmIds.size(); i++) {
      status[i] = getDataflowWorkerStatus(vmIds.get(i)) ;
      
    }
    return status ;
  }
  
  public TXEventBroadcaster getWorkerEventBroadcaster() { return workerEventBroadcaster; }
  
  public List<VMDescriptor> getActiveWorkers() throws RegistryException {
    List<String> activeWorkerIds = activeWorkers.getChildren();
    return allWorkers.getSelectRefChildrenAs(activeWorkerIds, VMDescriptor.class) ;
  }
  
  public List<String> getActiveWorkerIds() throws RegistryException {
    return activeWorkers.getChildren();
  }

  public int countActiveDataflowWorkers() throws RegistryException {
    return activeWorkers.getChildren().size();
  }

  public void addWorker(VMDescriptor vmDescriptor) throws RegistryException {
    Transaction transaction = registry.getTransaction() ;
    RefNode refNode = new RefNode(vmDescriptor.getRegistryPath()) ;
    transaction.createChild(allWorkers, vmDescriptor.getId(), refNode, NodeCreateMode.PERSISTENT) ;
    transaction.createDescendant(allWorkers, vmDescriptor.getId() + "/status", DataflowWorkerStatus.INIT, NodeCreateMode.PERSISTENT) ;
    transaction.createChild(activeWorkers, vmDescriptor.getId(), NodeCreateMode.PERSISTENT) ;
    transaction.commit();
  }
  
  public void setWorkerStatus(VMDescriptor vmDescriptor, DataflowWorkerStatus status) throws RegistryException {
    Node workerNode = allWorkers.getChild(vmDescriptor.getId());
    Node statusNode = workerNode.getChild("status");
    statusNode.setData(status);
  }
  
  public void setWorkerStatus(String vmId, DataflowWorkerStatus status) throws RegistryException {
    Node workerNode = allWorkers.getChild(vmId);
    Node statusNode = workerNode.getChild("status");
    statusNode.setData(status);
  }
  
  public void historyWorker(String vmId) throws RegistryException {
    Transaction transaction = registry.getTransaction() ;
    transaction.createChild(historyWorkers, vmId, NodeCreateMode.PERSISTENT) ;
    transaction.deleteChild(activeWorkers, vmId) ;
    transaction.commit();
  }
  
  public void createWorkerTaskExecutor(VMDescriptor vmDescriptor, DataflowTaskExecutorDescriptor descriptor) throws RegistryException {
    Node worker = allWorkers.getChild(vmDescriptor.getId()) ;
    Node executors = worker.createDescendantIfNotExists("executors");
    executors.createChild(descriptor.getId(), descriptor, NodeCreateMode.PERSISTENT);
  }
  
  public void updateWorkerTaskExecutor(VMDescriptor vmDescriptor, DataflowTaskExecutorDescriptor descriptor) throws RegistryException {
    Node worker = allWorkers.getChild(vmDescriptor.getId()) ;
    Node executor = worker.getDescendant("executors/" + descriptor.getId()) ;
    executor.setData(descriptor);
  }
  
  public DataflowWorkerStatus getDataflowWorkerStatus(String vmId) throws RegistryException {
    return allWorkers.getChild(vmId).getChild("status").getDataAs(DataflowWorkerStatus.class);
  }
  
  public List<DataflowTaskExecutorDescriptor> getWorkerExecutors(String worker) throws RegistryException {
    Node executors = allWorkers.getDescendant(worker + "/executors") ;
    return executors.getChildrenAs(DataflowTaskExecutorDescriptor.class);
  }
  
  public void waitForWorkerStatus(DataflowWorkerStatus status, long checkPeriod, long timeout) throws Exception {
    long stopTime = System.currentTimeMillis() + timeout;
    while(stopTime > System.currentTimeMillis()) {
      boolean ok = true;
      for(DataflowWorkerStatus selStatus : getActiveWorkerStatus()) {
        if(selStatus == null) continue;
        if(!selStatus.equalOrGreaterThan(status)) {
          ok = false;
          break;
        }
      }
      if(ok) return;
      Thread.sleep(checkPeriod);
    }
    throw new Exception("Not all dataflow worker have the " + status + ", after " + timeout + "ms");
  }
}