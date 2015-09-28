package com.neverwinterdp.scribengin.dataflow.registry;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.Transaction;

public class OperatorRegistry {
  private Registry registry;
  private String   dataflowPath;
  private Node     operatorsNode;

  public OperatorRegistry(Registry registry, String dataflowPath) throws RegistryException {
    this.registry         = registry;
    this.dataflowPath     = dataflowPath;
    operatorsNode = registry.get(dataflowPath + "/operators");
  }
  
  void create(Transaction transaction) throws RegistryException {
  }
  
  void initRegistry(Transaction transaction) throws RegistryException {
    transaction.create(operatorsNode, null, NodeCreateMode.PERSISTENT);
  }
}
