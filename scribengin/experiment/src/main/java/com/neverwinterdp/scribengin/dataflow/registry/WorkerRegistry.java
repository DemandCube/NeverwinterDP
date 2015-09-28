package com.neverwinterdp.scribengin.dataflow.registry;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.Transaction;

public class WorkerRegistry {
  private Registry registry ;
  private String   dataflowPath ;
  
  private Node workersNode;
  
  public WorkerRegistry(Registry registry, String dataflowPath) throws RegistryException {
    this.registry         = registry;
    this.dataflowPath     = dataflowPath;
    workersNode = registry.get(dataflowPath + "/workers");
  }
  
  void create(Transaction transaction) throws RegistryException {
  }
  
  void initRegistry(Transaction transaction) throws RegistryException {
    transaction.create(workersNode, null, NodeCreateMode.PERSISTENT);
  }
}
