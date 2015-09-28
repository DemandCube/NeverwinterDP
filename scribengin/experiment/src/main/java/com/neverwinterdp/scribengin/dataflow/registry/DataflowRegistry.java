package com.neverwinterdp.scribengin.dataflow.registry;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.Transaction;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.config.DataflowConfig;

public class DataflowRegistry {
  final static public String SCRIBENGIN_PATH       = "/scribengin";
  final static public String DATAFLOW_ALL_PATH     = SCRIBENGIN_PATH + "/dataflows/all";
  final static public String DATAFLOW_ACTIVE_PATH  = SCRIBENGIN_PATH + "/dataflows/active";
  final static public String DATAFLOW_HISTORY_PATH = SCRIBENGIN_PATH + "/dataflows/history";
  
  @Inject @Named("dataflow.registry.path") 
  private String             dataflowPath;
  
  @Inject
  private Registry           registry;
  
  private MasterRegistry     masterRegistry;
  
  private WorkerRegistry     workerRegistry ;
  
  public DataflowRegistry() {
  }
  
  public DataflowRegistry(Registry registry, String dataflowPath) {
    this.registry     = registry;
    this.dataflowPath = dataflowPath;
  }
  
  void init() throws RegistryException {
    masterRegistry = new MasterRegistry(registry, dataflowPath);
    workerRegistry = new WorkerRegistry(registry, dataflowPath);
  }
  
  public String create(Registry registry, DataflowConfig config) throws RegistryException {
    this.registry = registry;
    dataflowPath = DATAFLOW_ALL_PATH + "/" + config.getId();
    init();
    
    Node dataflowNode = registry.createIfNotExist(dataflowPath);
    Transaction transaction = registry.getTransaction();
    transaction.createChild(dataflowNode, "status", DataflowLifecycleStatus.CREATE, NodeCreateMode.PERSISTENT);
    transaction.createChild(dataflowNode, "config", config, NodeCreateMode.PERSISTENT);
    masterRegistry.create(transaction);
    workerRegistry.create(transaction);
    transaction.commit();
    return dataflowPath;
  }
  
  public void initRegistry() throws RegistryException {
    init();
    Transaction transaction = registry.getTransaction();
    masterRegistry.initRegistry(transaction);
    workerRegistry.initRegistry(transaction);
    transaction.commit();
  }
  
  static  public DataflowLifecycleStatus getStatus(Registry registry, String dataflowPath) throws RegistryException {
    return registry.getDataAs(dataflowPath + "/status" , DataflowLifecycleStatus.class) ;
  }
}
