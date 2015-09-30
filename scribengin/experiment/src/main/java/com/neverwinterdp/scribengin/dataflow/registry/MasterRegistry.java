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
  
  public MasterRegistry(Registry registry, String dataflowPath) throws RegistryException {
    this.registry         = registry;
    this.dataflowPath     = dataflowPath;
    this.masterNode       = registry.get(dataflowPath + "/master");
    this.masterLeaderNode = registry.get(dataflowPath + "/master/leader");
  }
  
  void create(Transaction transaction) throws RegistryException {
    transaction.create(masterNode.getPath(),new byte[0], NodeCreateMode.PERSISTENT);
    transaction.create(masterLeaderNode.getPath(),new byte[0], NodeCreateMode.PERSISTENT);
  }
  
  void initRegistry(Transaction transaction) throws RegistryException {
  }
}
