package com.neverwinterdp.scribengin.dataflow.registry;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.Transaction;
import com.neverwinterdp.registry.txevent.TXEventBroadcaster;

public class MasterRegistry {
  final static public String MASTER_EVENT_PATH  = "master/events" ;
  
  private Registry registry ;
  private String   dataflowPath ;
  
  private Node     masterNode ;
  private Node     masterLeaderNode ;
  private Node     activitiesNode;
  
  private TXEventBroadcaster masterEventBroadcaster;
  
  public MasterRegistry(Registry registry, String dataflowPath) throws RegistryException {
    this.registry         = registry;
    this.dataflowPath     = dataflowPath;
    this.masterNode       = registry.get(dataflowPath + "/master");
    this.masterLeaderNode = registry.get(dataflowPath + "/master/leader");
    this.activitiesNode   = registry.get(dataflowPath + "/master/activities");
    masterEventBroadcaster = new TXEventBroadcaster(registry, dataflowPath + "/" + MASTER_EVENT_PATH, false);
  }
  
  void create(Transaction transaction) throws RegistryException {
    transaction.create(masterNode.getPath(),new byte[0], NodeCreateMode.PERSISTENT);
    transaction.create(masterLeaderNode.getPath(),new byte[0], NodeCreateMode.PERSISTENT);
    masterEventBroadcaster.initRegistry(transaction);
  }
  
  void initRegistry(Transaction transaction) throws RegistryException {
  }
  
  public String getActivitiesNodePath() { return activitiesNode.getPath(); }
  
  public Node getActivitiesNode() { return activitiesNode ; }
  
  public TXEventBroadcaster getMaserEventBroadcaster() { return masterEventBroadcaster; }
}
