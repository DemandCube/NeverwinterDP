package com.neverwinterdp.scribengin.dataflow.registry;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.Transaction;
import com.neverwinterdp.scribengin.dataflow.config.DataflowConfig;

public class ConfigRegistry {
  private Registry       registry;
  private String         dataflowPath;
  private Node           configNode;
  private DataflowConfig config;

  public ConfigRegistry(Registry registry, String dataflowPath) throws RegistryException {
    this.registry         = registry;
    this.dataflowPath     = dataflowPath;
    configNode = registry.get(dataflowPath + "/config");
  }
  
  void create(Transaction transaction) throws RegistryException {
  }
  
  void initRegistry(Transaction transaction) throws RegistryException {
  }
  
  public DataflowConfig getDataflowConfig() throws RegistryException {
    return getDataflowConfig(false);
  }
  
  public DataflowConfig getDataflowConfig(boolean refresh) throws RegistryException {
    if(config == null || refresh) {
      config = configNode.getDataAs(DataflowConfig.class);
    }
    return config;
  }
}