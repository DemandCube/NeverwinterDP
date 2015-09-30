package com.neverwinterdp.scribengin.dataflow.registry;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.Transaction;

public class StreamRegistry {
  private Registry registry ;
  private String   dataflowPath ;
  
  private Node     streamsNode ;
  
  public StreamRegistry(Registry registry, String dataflowPath) throws RegistryException {
    this.registry       = registry;
    this.dataflowPath   = dataflowPath;
    this.streamsNode    = registry.get(dataflowPath + "/streams");
  }
  
  void create(Transaction transaction) throws RegistryException {
  }
  
  void initRegistry(Transaction transaction) throws RegistryException {
    transaction.create(streamsNode, null, NodeCreateMode.PERSISTENT);
  }
}
