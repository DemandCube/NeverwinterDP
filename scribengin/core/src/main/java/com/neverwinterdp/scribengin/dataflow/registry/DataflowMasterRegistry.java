package com.neverwinterdp.scribengin.dataflow.registry;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.queue.DistributedQueue;
import com.neverwinterdp.registry.txevent.TXEventBroadcaster;
import com.neverwinterdp.vm.VMDescriptor;

public class DataflowMasterRegistry {
  final static public String MASTER_PATH                   = "master";
  final static public String MASTER_LEADER_PATH            = MASTER_PATH + "/leader";
  final static public String MASTER_EVENT_PATH             = MASTER_PATH + "/events";
  final static public String FAILURE_SIMULATION_EVENT_PATH = MASTER_PATH + "/failure-simulation-events" ;
  
  private Registry           registry;
  private String             dataflowPath;
  private Node               masterLeaderNode;
  private Node               masterEventNode;
  private TXEventBroadcaster masterEventBroadcaster;
  private DistributedQueue   failureEventQueue ;
  
  public DataflowMasterRegistry(Registry registry, String dataflowPath) throws RegistryException {
    this.registry = registry;
    this.dataflowPath = dataflowPath ;
    masterLeaderNode = registry.get(dataflowPath + "/" + MASTER_LEADER_PATH);
    masterEventNode  = registry.get(dataflowPath + "/" + MASTER_EVENT_PATH);
    masterEventBroadcaster = 
      new TXEventBroadcaster(registry, dataflowPath + "/" + MASTER_EVENT_PATH, false);
    failureEventQueue = new DistributedQueue(registry, dataflowPath + "/" + FAILURE_SIMULATION_EVENT_PATH, false) ;
  }
  
  public void initRegistry() throws Exception {
    masterLeaderNode.createIfNotExists();
    masterEventNode.createIfNotExists();
    masterEventBroadcaster.initRegistry();
    failureEventQueue.initRegistry();
  }
  
  public Node getMasterEventNode() { return masterEventNode ; }
  
  public TXEventBroadcaster getMasterEventBroadcaster() { return masterEventBroadcaster; }
  
  public DistributedQueue getFailureEventQueue() { return failureEventQueue; }
  
  public VMDescriptor getDataflowMaster() throws RegistryException {
    String leaderPath = dataflowPath + "/" + MASTER_LEADER_PATH;
    Node node = registry.getRef(leaderPath);
    return node.getDataAs(VMDescriptor.class);
  }
  
  public int countDataflowMasters() throws RegistryException {
    return registry.getChildren(dataflowPath + "/" + MASTER_LEADER_PATH).size();
  }
}