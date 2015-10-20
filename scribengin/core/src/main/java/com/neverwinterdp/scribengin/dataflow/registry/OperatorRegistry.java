package com.neverwinterdp.scribengin.dataflow.registry;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.Transaction;
import com.neverwinterdp.scribengin.dataflow.config.OperatorConfig;

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
  
  public void create(String name, OperatorConfig config) throws RegistryException {
    Transaction transaction = registry.getTransaction();
    String operatorPath = operatorsNode.getPath() + "/" + name;
    String inputsPath  = operatorPath + "/inputs";
    String outputsPath = operatorPath + "/outputs";
    transaction.create(operatorPath, config, NodeCreateMode.PERSISTENT);
    transaction.create(inputsPath, null, NodeCreateMode.PERSISTENT);
    transaction.create(outputsPath, null, NodeCreateMode.PERSISTENT);
    for(String input : config.getInputs()) {
      transaction.create(inputsPath + "/" + input, null, NodeCreateMode.PERSISTENT);
    }
    for(String output : config.getOutputs()) {
      transaction.create(outputsPath + "/" + output, null, NodeCreateMode.PERSISTENT);
    }
    transaction.commit();
  }
}
