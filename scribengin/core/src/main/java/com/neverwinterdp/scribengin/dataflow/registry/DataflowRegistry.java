package com.neverwinterdp.scribengin.dataflow.registry;

import java.util.ArrayList;
import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.neverwinterdp.registry.ErrorCode;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.Transaction;
import com.neverwinterdp.registry.activity.ActivityRegistry;
import com.neverwinterdp.registry.notification.Notifier;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.api.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.operator.OperatorTaskRuntimeReport;
import com.neverwinterdp.scribengin.dataflow.worker.DataflowWorkerRuntimeReport;

@Singleton
public class DataflowRegistry {
  final static public String SCRIBENGIN_PATH       = "/scribengin";
  final static public String DATAFLOWS_PATH        = SCRIBENGIN_PATH + "/dataflows";
  final static public String DATAFLOW_ALL_PATH     = SCRIBENGIN_PATH + "/dataflows/all";
  final static public String DATAFLOW_ACTIVE_PATH  = SCRIBENGIN_PATH + "/dataflows/active";
  final static public String DATAFLOW_HISTORY_PATH = SCRIBENGIN_PATH + "/dataflows/history";
  
  final static public String NOTIFICATIONS_PATH    = "notifications";
  
  final static public String  DATAFLOWS_ID_TRACKERS       = DATAFLOWS_PATH   + "/id-trackers";
  final static public String  DATAFLOW_ID_TRACKER         = DATAFLOWS_ID_TRACKERS + "/dataflow-id";
  final static public String  DATAFLOW_MASTER_ID_TRACKER  = DATAFLOWS_ID_TRACKERS + "/dataflow-master-id";
  final static public String  DATAFLOW_WORKER_ID_TRACKER  = DATAFLOWS_ID_TRACKERS + "/dataflow-worker-id";
  
  
  @Inject
  @Named("dataflow.registry.path")
  private String dataflowPath;

  @Inject
  private Registry registry;

  private Node statusNode;

  private ConfigRegistry       configRegistry;
  private StreamRegistry       streamRegistry;
  private OperatorRegistry     operatorRegistry;
  private MasterRegistry       masterRegistry;
  private WorkerRegistry       workerRegistry;
  private DataflowTaskRegistry taskRegistry;

  private Notifier dataflowTaskNotifier;
  private Notifier dataflowWorkerNotifier;
  
  public DataflowRegistry() {
  }
  
  public DataflowRegistry(Registry registry, String dataflowPath) throws RegistryException {
    this.registry     = registry;
    this.dataflowPath = dataflowPath;
    init();
  }
  
  void init() throws RegistryException {
    statusNode = registry.get(dataflowPath + "/status") ;
    configRegistry   = new ConfigRegistry(registry, dataflowPath);
    streamRegistry   = new StreamRegistry(registry, dataflowPath);
    operatorRegistry = new OperatorRegistry(registry, dataflowPath);
    masterRegistry   = new MasterRegistry(registry, dataflowPath);
    workerRegistry   = new WorkerRegistry(registry, dataflowPath);
    
    taskRegistry = new DataflowTaskRegistry(registry, dataflowPath);
    
    String notificationPath = dataflowPath +  "/" + NOTIFICATIONS_PATH;
    dataflowTaskNotifier   = new Notifier(registry, notificationPath, "dataflow-tasks");
    dataflowWorkerNotifier = new Notifier(registry, notificationPath, "dataflow-workers");
  }
  
  @Inject
  public void inInject() throws RegistryException {
    init();
  }
  
  public String create(Registry registry, DataflowDescriptor config) throws RegistryException {
    this.registry = registry;
    dataflowPath = DATAFLOW_ALL_PATH + "/" + config.getId();
    init();
    
    Node dataflowNode = registry.createIfNotExist(dataflowPath);
    Transaction transaction = registry.getTransaction();
    transaction.createChild(dataflowNode, "status", DataflowLifecycleStatus.CREATE, NodeCreateMode.PERSISTENT);
    transaction.createChild(dataflowNode, "config", config, NodeCreateMode.PERSISTENT);
    configRegistry.create(transaction);
    streamRegistry.create(transaction);
    operatorRegistry.create(transaction);
    masterRegistry.create(transaction);
    workerRegistry.create(transaction);
    transaction.commit();
    return dataflowPath;
  }
  
  public void initRegistry() throws RegistryException {
    try {
      init();
      Transaction transaction = registry.getTransaction();
      configRegistry.initRegistry(transaction);
      streamRegistry.initRegistry(transaction);
      operatorRegistry.initRegistry(transaction);
      masterRegistry.initRegistry(transaction);
      workerRegistry.initRegistry(transaction);
      taskRegistry.initRegistry(transaction);

      String notificationPath = dataflowPath +  "/" + NOTIFICATIONS_PATH;
      transaction.create(notificationPath, new byte[0], NodeCreateMode.PERSISTENT);
      dataflowTaskNotifier.initRegistry(transaction);
      dataflowWorkerNotifier.initRegistry(transaction);
      transaction.commit();
    } catch(Throwable ex) {
      ex.printStackTrace();
      throw ex;
    }
  }

  public String getDataflowPath() { return this.dataflowPath; }
  
  public Registry getRegistry() { return this.registry ; }
  
  public ConfigRegistry getConfigRegistry() { return configRegistry; }
  
  public StreamRegistry getStreamRegistry() { return streamRegistry ; }
  
  public OperatorRegistry getOperatorRegistry() { return operatorRegistry ; }
  
  public MasterRegistry getMasterRegistry() { return masterRegistry; }
  
  public WorkerRegistry getWorkerRegistry() { return workerRegistry; }
  
  public DataflowTaskRegistry getTaskRegistry() { return taskRegistry; }
  
  public Notifier getDataflowTaskNotifier() { return this.dataflowTaskNotifier ; }
  
  public Notifier getDataflowWorkerNotifier() { return this.dataflowWorkerNotifier ; }
  
  public DataflowLifecycleStatus getStatus() throws RegistryException {
    return statusNode.getDataAs(DataflowLifecycleStatus.class) ;
  }
  
  public void setStatus(DataflowLifecycleStatus status) throws RegistryException {
    statusNode.setData(status);
  }
  
  static  public DataflowLifecycleStatus getStatus(Registry registry, String dataflowPath) throws RegistryException {
    return registry.getDataAs(dataflowPath + "/status" , DataflowLifecycleStatus.class) ;
  }
  
  static public ActivityRegistry getMasterActivityRegistry(Registry registry, String dataflowPath) throws RegistryException {
    return new ActivityRegistry(registry, dataflowPath + "/master/activities") ;
  }
  
  static public List<DataflowWorkerRuntimeReport> getAllDataflowWorkerRuntimeReports(Registry registry, String dataflowPath) throws RegistryException {
    return getDataflowWorkerRuntimeReports(registry, dataflowPath, "all");
  }
  
  static public List<DataflowWorkerRuntimeReport> getActiveDataflowWorkerRuntimeReports(Registry registry, String dataflowPath) throws RegistryException {
    return getDataflowWorkerRuntimeReports(registry, dataflowPath, "active");
  }
  
  static public List<DataflowWorkerRuntimeReport> getHistoryDataflowWorkerRuntimeReports(Registry registry, String dataflowPath) throws RegistryException {
    return getDataflowWorkerRuntimeReports(registry, dataflowPath, "history");
  }
  
  static public List<DataflowWorkerRuntimeReport> getDataflowWorkerRuntimeReports(Registry registry, String dataflowPath, String category) throws RegistryException {
    try {
      String workerAllPath = dataflowPath + "/workers/all";
      String workerListPath = dataflowPath + "/workers/" + category;
      List<String> workerIds = registry.getChildren(workerListPath) ;
      List<DataflowWorkerRuntimeReport> holder = new ArrayList<>();
      for(String selWorkerId : workerIds) {
        holder.add(new DataflowWorkerRuntimeReport(registry, workerAllPath + "/" + selWorkerId));
      }
      return holder;
    } catch(RegistryException ex) {
      if(ex.getErrorCode() == ErrorCode.NoNode) return new ArrayList<>();
      throw ex;
    }
  }
}