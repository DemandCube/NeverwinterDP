package com.neverwinterdp.scribengin.dataflow.registry;

import java.util.ArrayList;
import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.neverwinterdp.message.TrackingWindowRegistry;
import com.neverwinterdp.registry.ErrorCode;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.RefNode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.SequenceIdTracker;
import com.neverwinterdp.registry.Transaction;
import com.neverwinterdp.registry.activity.ActivityRegistry;
import com.neverwinterdp.registry.notification.Notifier;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.runtime.master.DataflowMasterRuntimeReport;
import com.neverwinterdp.scribengin.dataflow.runtime.worker.DataflowWorkerRuntimeReport;
import com.neverwinterdp.scribengin.dataflow.tracking.TrackingRegistry;
import com.neverwinterdp.vm.VMDescriptor;

@Singleton
public class DataflowRegistry {
  final static public String SCRIBENGIN_PATH       = "/scribengin";
  final static public String DATAFLOWS_PATH        = SCRIBENGIN_PATH + "/dataflows";
  final static public String DATAFLOW_ALL_PATH     = SCRIBENGIN_PATH + "/dataflows/all";
  final static public String DATAFLOW_ACTIVE_PATH  = SCRIBENGIN_PATH + "/dataflows/active";
  final static public String DATAFLOW_HISTORY_PATH = SCRIBENGIN_PATH + "/dataflows/history";
  
  final static public String DATAFLOW_STATUS       = "dataflow-status";
  final static public String REGISTRY_STATUS       = "registry-status";
  final static public String NOTIFICATIONS_PATH    = "notifications";
  
  static public enum RegistryStatus { Create, InitStructure, Ready } 
  
  @Inject
  @Named("dataflow.registry.path")
  private String dataflowPath;

  @Inject
  private Registry registry;

  private Node  registryStatusNode;
  private Node  dataflowStatusNode;

  private ConfigRegistry          configRegistry;
  private StreamRegistry          streamRegistry;
  private OperatorRegistry        operatorRegistry;
  private MasterRegistry          masterRegistry;
  private WorkerRegistry          workerRegistry;
  private DataflowTaskRegistry    taskRegistry;
  private TrackingWindowRegistry  messageTrackingRegistry;

  private SequenceIdTracker       workerIdTracker;
  private SequenceIdTracker       masterIdTracker;
  
  private Notifier dataflowTaskNotifier;
  private Notifier dataflowWorkerNotifier;
  
  public DataflowRegistry() {
  }
  
  
  public DataflowRegistry(Registry registry, DataflowDescriptor config) throws RegistryException {
    this.registry = registry;
    dataflowPath = DATAFLOW_ALL_PATH + "/" + config.getId();
    init();

    if(!registry.exists(dataflowPath)) {
      Node dataflowNode = registry.createIfNotExist(dataflowPath);
      Transaction transaction = registry.getTransaction();
      transaction.createChild(dataflowNode, REGISTRY_STATUS, RegistryStatus.Create, NodeCreateMode.PERSISTENT);
      transaction.createChild(dataflowNode, DATAFLOW_STATUS, DataflowLifecycleStatus.CREATE, NodeCreateMode.PERSISTENT);
      transaction.createChild(dataflowNode, "config", config, NodeCreateMode.PERSISTENT);
      configRegistry.create(transaction);
      streamRegistry.create(transaction);
      operatorRegistry.create(transaction);
      masterRegistry.create(transaction);
      workerRegistry.create(transaction);
      
      String idTrackerPath = dataflowPath +  "/id-tracker"  ;
      transaction.create(idTrackerPath, new byte[0], NodeCreateMode.PERSISTENT);
      masterIdTracker.initRegistry(transaction);
      workerIdTracker.initRegistry(transaction);
      transaction.commit();
    }
  }
  
  
  public DataflowRegistry(Registry registry, String dataflowPath) throws RegistryException {
    this.registry     = registry;
    this.dataflowPath = dataflowPath;
    init();
  }
  
  void init() throws RegistryException {
    dataflowStatusNode       = registry.get(dataflowPath + "/" + DATAFLOW_STATUS) ;
    registryStatusNode       = registry.get(dataflowPath + "/" + REGISTRY_STATUS) ;
    configRegistry   = new ConfigRegistry(registry, dataflowPath);
    streamRegistry   = new StreamRegistry(registry, dataflowPath);
    operatorRegistry = new OperatorRegistry(registry, dataflowPath);
    masterRegistry   = new MasterRegistry(registry, dataflowPath);
    workerRegistry   = new WorkerRegistry(registry, dataflowPath);
    
    taskRegistry = new DataflowTaskRegistry(registry, dataflowPath);
    
    messageTrackingRegistry = new TrackingWindowRegistry(registry, dataflowPath + "/message-tracking");
    
    String idTrackerPath = dataflowPath +  "/id-tracker"  ;
    masterIdTracker = new SequenceIdTracker(registry, idTrackerPath + "/master", false);
    workerIdTracker = new SequenceIdTracker(registry, idTrackerPath + "/worker", false);
    
    String notificationPath = dataflowPath +  "/" + NOTIFICATIONS_PATH;
    dataflowTaskNotifier   = new Notifier(registry, notificationPath, "dataflow-tasks");
    dataflowWorkerNotifier = new Notifier(registry, notificationPath, "dataflow-workers");
  }
  
  @Inject
  public void inInject() throws RegistryException {
    init();
  }
  
  public void initRegistry() throws RegistryException {
    try {
      RegistryStatus registryStatus = getRegistryStatus();
      if(registryStatus != RegistryStatus.Create) return;
      
      Transaction transaction = registry.getTransaction();
      configRegistry.initRegistry(transaction);
      streamRegistry.initRegistry(transaction);
      operatorRegistry.initRegistry(transaction);
      masterRegistry.initRegistry(transaction);
      workerRegistry.initRegistry(transaction);
      taskRegistry.initRegistry(transaction);

      messageTrackingRegistry.initRegistry(transaction);

      String notificationPath = dataflowPath + "/" + NOTIFICATIONS_PATH;
      transaction.create(notificationPath, new byte[0], NodeCreateMode.PERSISTENT);
      dataflowTaskNotifier.initRegistry(transaction);
      dataflowWorkerNotifier.initRegistry(transaction);
      transaction.setData(registryStatusNode, RegistryStatus.InitStructure);
      transaction.commit();
    } catch(Throwable ex) {
      ex.printStackTrace();
      throw ex;
    }
  }

  public String getDataflowPath() { return this.dataflowPath; }
  
  public Registry getRegistry() { return this.registry ; }
  
  public RegistryStatus getRegistryStatus() throws RegistryException {
    return registryStatusNode.getDataAs(RegistryStatus.class) ;
  }
  
  public void setRegistryReadyStatus() throws RegistryException {
    registryStatusNode.setData(RegistryStatus.Ready);
  }
  
  public DataflowLifecycleStatus getDataflowStatus() throws RegistryException {
    return dataflowStatusNode.getDataAs(DataflowLifecycleStatus.class) ;
  }
  
  public void setDataflowStatus(DataflowLifecycleStatus status) throws RegistryException {
    dataflowStatusNode.setData(status);
  }
  
  
  public ConfigRegistry getConfigRegistry() { return configRegistry; }
  
  public StreamRegistry getStreamRegistry() { return streamRegistry ; }
  
  public OperatorRegistry getOperatorRegistry() { return operatorRegistry ; }
  
  public MasterRegistry getMasterRegistry() { return masterRegistry; }
  
  public WorkerRegistry getWorkerRegistry() { return workerRegistry; }
  
  public DataflowTaskRegistry getTaskRegistry() { return taskRegistry; }
  
  public TrackingWindowRegistry getMessageTrackingRegistry() { return messageTrackingRegistry; }
  
  public SequenceIdTracker getMasterIdTracker() { return masterIdTracker; }
  
  public SequenceIdTracker getWorkerIdTracker() { return workerIdTracker; }
  
  public Notifier getDataflowTaskNotifier() { return this.dataflowTaskNotifier ; }
  
  public Notifier getDataflowWorkerNotifier() { return this.dataflowWorkerNotifier ; }
  
  static  public DataflowLifecycleStatus getDataflowStatus(Registry registry, String dataflowPath) throws RegistryException {
    return registry.getDataAs(dataflowPath + "/" + DATAFLOW_STATUS , DataflowLifecycleStatus.class) ;
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
  static public List<DataflowMasterRuntimeReport> getDataflowMasterRuntimeReports(Registry registry, String dataflowPath) throws RegistryException {
    Node masterLeaderNode = registry.get(dataflowPath + "/master/leader");
    RefNode vmLeaderRef = masterLeaderNode.getDataAs(RefNode.class);
    VMDescriptor vmLeader = registry.getDataAs(vmLeaderRef.getPath(), VMDescriptor.class);
    
    List<RefNode> refChildren = masterLeaderNode.getChildrenAs(RefNode.class);
    List<DataflowMasterRuntimeReport> holder = new ArrayList<>();
    for(int i = 0; i < refChildren.size(); i++) {
      RefNode refNode = refChildren.get(i);
      VMDescriptor vmDescriptor = registry.getDataAs(refNode.getPath(), VMDescriptor.class);
      boolean leader = false;
      if(vmLeader != null) leader = vmLeader.getId().equals(vmDescriptor.getId());
      DataflowMasterRuntimeReport report = new DataflowMasterRuntimeReport();
      report.setVmId(vmDescriptor.getId());
      report.setLeader(leader);
      holder.add(report);
    }
    return holder;
  }
  
  static public List<DataflowWorkerRuntimeReport> getDataflowWorkerRuntimeReports(Registry registry, String dataflowPath, String category) throws RegistryException {
    try {
      String workerAllPath  = dataflowPath + "/workers/all";
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