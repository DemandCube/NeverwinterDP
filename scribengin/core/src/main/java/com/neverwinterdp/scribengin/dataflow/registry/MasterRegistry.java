package com.neverwinterdp.scribengin.dataflow.registry;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.Transaction;

public class MasterRegistry {
  private Registry registry ;
  private String   dataflowPath ;
  
  private Node     masterNode ;
  private Node     masterLeaderNode ;
  private Node     activitiesNode;
  
  public MasterRegistry(Registry registry, String dataflowPath) throws RegistryException {
    this.registry         = registry;
    this.dataflowPath     = dataflowPath;
    this.masterNode       = registry.get(dataflowPath + "/master");
    this.masterLeaderNode = registry.get(dataflowPath + "/master/leader");
    this.activitiesNode   = registry.get(dataflowPath + "/master/activities");
  }
  
  void create(Transaction transaction) throws RegistryException {
    transaction.create(masterNode.getPath(),new byte[0], NodeCreateMode.PERSISTENT);
    transaction.create(masterLeaderNode.getPath(),new byte[0], NodeCreateMode.PERSISTENT);
  }
  
  void initRegistry(Transaction transaction) throws RegistryException {
  }
  
  public String getActivitiesNodePath() { return activitiesNode.getPath(); }
  
  public Node getActivitiesNode() { return activitiesNode ; }
  
}
