package com.neverwinterdp.scribengin.dataflow.registry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.neverwinterdp.registry.DataMapperCallback;
import com.neverwinterdp.registry.ErrorCode;
import com.neverwinterdp.registry.MultiDataGet;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.activity.ActivityRegistry;
import com.neverwinterdp.registry.notification.Notifier;
import com.neverwinterdp.registry.task.TaskContext;
import com.neverwinterdp.registry.task.TaskRegistry;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskReport;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskRuntimeReport;
import com.neverwinterdp.scribengin.dataflow.DataflowWorkerRuntimeReport;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.yara.MetricRegistry;
import com.neverwinterdp.yara.snapshot.MetricRegistrySnapshot;

@Singleton
public class DataflowRegistry {
  final static public String ACTIVITIES_PATH        = "activities";
  
  final static public String NOTIFICATIONS_PATH     = "notifications";
  
  final static public DataMapperCallback<DataflowTaskDescriptor> TASK_DESCRIPTOR_DATA_MAPPER = new DataMapperCallback<DataflowTaskDescriptor>() {
    @Override
    public DataflowTaskDescriptor map(String path, byte[] data, Class<DataflowTaskDescriptor> type) {
      DataflowTaskDescriptor descriptor = JSONSerializer.INSTANCE.fromBytes(data, type);
      descriptor.setRegistryPath(path);
      return descriptor;
    }
  };
  
  @Inject @Named("dataflow.registry.path") 
  private String             dataflowPath;
  @Inject
  private Registry           registry;
  
  @Inject
  private VMDescriptor       vmDescriptor;
  
  private DataflowDescriptor dataflowDescriptor;
  
  private ConfigurationRegistry  configuration ;
  
  private DataflowMasterRegistry masterRegistry ;
  private DataflowWorkerRegistry workerRegistry ;
  private TaskRegistry<DataflowTaskDescriptor> taskRegistry ;
  
  private Node               statusNode;

  private Node               activeActivitiesNode;
  
  private Node               metricsNode ;
  
  private Notifier           dataflowTaskNotifier ;
  private Notifier           dataflowWorkerNotifier ;

  public DataflowRegistry() { }
  
  public DataflowRegistry(Registry registry, String dataflowPath) throws Exception { 
    this.registry = registry;
    this.dataflowPath = dataflowPath;
    onInit();
  }
  
  @Inject
  public void onInit() throws Exception {
    configuration = new ConfigurationRegistry(dataflowPath);
    masterRegistry = new DataflowMasterRegistry(registry, dataflowPath);
    workerRegistry = new DataflowWorkerRegistry(registry, dataflowPath);
    
    String taskPath = dataflowPath + "/tasks";
    taskRegistry = new TaskRegistry<DataflowTaskDescriptor>(registry, taskPath, DataflowTaskDescriptor.class);
    
    
    statusNode = registry.get(dataflowPath + "/status");

   
    activeActivitiesNode = registry.get(dataflowPath + "/" + ACTIVITIES_PATH);
    metricsNode = registry.get(dataflowPath + "/metrics");
    
    String notificationPath = getDataflowNotificationsPath() ;
    dataflowTaskNotifier = new Notifier(registry, notificationPath, "dataflow-tasks");
    dataflowWorkerNotifier = new Notifier(registry, notificationPath, "dataflow-workers");
  }
  
  public boolean initRegistry() throws Exception {
    dataflowDescriptor = getDataflowDescriptor() ;
    if(statusNode.exists()) {
      return false;
    }
    configuration.initRegistry(dataflowDescriptor);
    masterRegistry.initRegistry();
    workerRegistry.initRegistry();
    
    statusNode.createIfNotExists();
    
    activeActivitiesNode.createIfNotExists();
    
    metricsNode.createIfNotExists();
    
    dataflowTaskNotifier.initRegistry();
    dataflowWorkerNotifier.initRegistry();
    setStatus(DataflowLifecycleStatus.INIT);
    return true;
  }
  
  public String getDataflowPath() { return this.dataflowPath ; }
  
  public ConfigurationRegistry getConfiguration() { return this.configuration ;}
  
  public DataflowMasterRegistry getMasterRegistry() { return this.masterRegistry;}
  
  public DataflowWorkerRegistry getWorkerRegistry() { return this.workerRegistry;}
  
  public String getDataflowNotificationsPath() { return this.dataflowPath  + "/" + NOTIFICATIONS_PATH; }
  
  public Node getTasksFinishedNode() { return taskRegistry.getTasksFinishedNode();}
  
  public Node getTasksAssignedNode() { return taskRegistry.getTasksAssignedNode(); }
  
  public Node getTasksAssignedHeartbeatNode() { return taskRegistry.getTasksAssignedHeartbeatNode(); }
  
  public Node getActiveActivitiesNode() { return activeActivitiesNode; } 
  
  public Notifier getDataflowTaskNotifier() { return this.dataflowTaskNotifier ; }
  
  public Notifier getDataflowWorkerNotifier() { return this.dataflowWorkerNotifier ; }
  
  public Registry getRegistry() { return this.registry ; }
  
  public TaskRegistry<DataflowTaskDescriptor> getTaskRegistry() { return this.taskRegistry; }
  
  public DataflowDescriptor getDataflowDescriptor() throws RegistryException {
    return getDataflowDescriptor(true);
  }
  
  public DataflowDescriptor getDataflowDescriptor(boolean reload) throws RegistryException {
    if(reload) dataflowDescriptor = registry.getDataAs(dataflowPath, DataflowDescriptor.class);
    return dataflowDescriptor;
  }
  
  public void updateDataflowDescriptor(DataflowDescriptor descriptor) throws RegistryException {
    registry.setData(dataflowPath, descriptor);
    this.dataflowDescriptor = descriptor ;
  }
  
  public Node getStatusNode() { return this.statusNode ; }
  
  public DataflowLifecycleStatus getStatus() throws RegistryException {
    return statusNode.getDataAs(DataflowLifecycleStatus.class) ;
  }
  
  public void setStatus(DataflowLifecycleStatus event) throws RegistryException {
    statusNode.setData(event);
  }
  
  public void addAvailableTask(DataflowTaskDescriptor taskDescriptor) throws RegistryException {
    String taskId = taskDescriptor.getTaskId();
    Node taskNode = taskRegistry.getTasksListNode().getChild(taskId);
    taskDescriptor.setRegistryPath(taskNode.getPath());
    taskRegistry.offer(taskId, taskDescriptor);
    create(taskDescriptor, new DataflowTaskReport(taskDescriptor.getTaskId()));
  }

  public TaskContext<DataflowTaskDescriptor> dataflowTaskAssign(final VMDescriptor vmDescriptor) throws RegistryException  {
    TaskContext<DataflowTaskDescriptor> tContext = taskRegistry.take(vmDescriptor.getRegistryPath());
    return tContext;
  }
  
  public void dataflowTaskSuspend(final TaskContext<DataflowTaskDescriptor> context) throws RegistryException {
    dataflowTaskSuspend(context, false) ;
  }
  
  public void dataflowTaskSuspend(final TaskContext<DataflowTaskDescriptor> context, final boolean disconnectHeartbeat) throws RegistryException {
    taskRegistry.suspend(vmDescriptor.getRegistryPath(), context.getTaskTransactionId(), disconnectHeartbeat);
  }

  public void dataflowTaskFinish(final TaskContext<DataflowTaskDescriptor> context) throws RegistryException {
    taskRegistry.finish(vmDescriptor.getRegistryPath(), context.getTaskTransactionId());
  }
  
  public void dataflowTaskReport(DataflowTaskDescriptor descriptor, DataflowTaskReport report) throws RegistryException {
    Node  reportNode = taskRegistry.getTasksListNode().getChild(descriptor.getTaskId()).getChild("report");
    reportNode.setData(report);
  }
  
  public void create(DataflowTaskDescriptor descriptor, DataflowTaskReport report) throws RegistryException {
    Node taskNode = taskRegistry.getTasksListNode().getChild(descriptor.getTaskId());
    taskNode.createChild("report", report, NodeCreateMode.PERSISTENT);
  }
  
  public List<DataflowTaskDescriptor> getTaskDescriptors() throws RegistryException {
    return taskRegistry.getTasksListNode().getChildrenAs(DataflowTaskDescriptor.class, TASK_DESCRIPTOR_DATA_MAPPER);
  }
  
  public DataflowTaskDescriptor getTaskDescriptor(String taskName) throws RegistryException {
    return taskRegistry.getTasksListNode().getChild(taskName).getDataAs(DataflowTaskDescriptor.class, TASK_DESCRIPTOR_DATA_MAPPER);
  }
  
  public DataflowTaskReport getTaskReport(DataflowTaskDescriptor descriptor) throws RegistryException {
    Node taskNode = taskRegistry.getTasksListNode().getChild(descriptor.getTaskId());
    return registry.getDataAs(taskNode.getPath() + "/report", DataflowTaskReport.class) ;
  }
  
  public List<DataflowTaskReport> getTaskReports(List<DataflowTaskDescriptor> descriptors) throws RegistryException {
    List<String> reportPaths = new ArrayList<String>();
    for(int i = 0; i < descriptors.size(); i++) {
      DataflowTaskDescriptor descriptor = descriptors.get(i);
      Node taskNode = taskRegistry.getTasksListNode().getChild(descriptor.getTaskId());
      reportPaths.add(taskNode.getPath() + "/report") ;
    }
    return registry.getDataAs(reportPaths, DataflowTaskReport.class) ;
  }
  
  public void saveMetric(String vmName, MetricRegistry mRegistry) throws RegistryException {
    MetricRegistrySnapshot mRegistrySnapshot = new MetricRegistrySnapshot(vmName, mRegistry) ;
    if(!metricsNode.hasChild(vmName)) {
      metricsNode.createChild(vmName, mRegistrySnapshot, NodeCreateMode.PERSISTENT);
    } else {
      metricsNode.getChild(vmName).setData( mRegistrySnapshot);
    }
  }
  
  public MetricRegistrySnapshot getMetric(String vmName) throws RegistryException {
    MetricRegistrySnapshot mRegistrySnapshot = metricsNode.getChild(vmName).getDataAs(MetricRegistrySnapshot.class) ;
    return mRegistrySnapshot;
  }
  
  public List<MetricRegistrySnapshot> getMetrics() throws RegistryException {
    return metricsNode.getChildrenAs(MetricRegistrySnapshot.class);
  }
  
  public ActivityRegistry getActivityRegistry() throws RegistryException {
    return new ActivityRegistry(registry, dataflowPath + "/" + ACTIVITIES_PATH) ;
  }

  public boolean waitForDataflowStatus(DataflowLifecycleStatus status, long timeout) throws Exception {
    long stopTime = System.currentTimeMillis() + timeout ;
    while(System.currentTimeMillis() < stopTime) {
      if(status == getStatus()) return true;
      Thread.sleep(1000);
    }
    dump();
    return false ;
  }
 
  
  public void dump() throws RegistryException, IOException {
    registry.get(dataflowPath).dump(System.out);
  }
  
  static  public DataflowLifecycleStatus getStatus(Registry registry, String dataflowPath) throws RegistryException {
    return registry.getDataAs(dataflowPath + "/status" , DataflowLifecycleStatus.class) ;
  }
  
  static public List<DataflowDescriptor> getDataflowDescriptors(Registry registry, String listPath) throws RegistryException {
    MultiDataGet<DataflowDescriptor> multiGet = registry.createMultiDataGet(DataflowDescriptor.class);
    multiGet.getChildren(listPath);
    multiGet.shutdown();
    multiGet.waitForAllGet(30000);
    return multiGet.getResults();
  }
  
  static public List<DataflowTaskDescriptor> getDataflowTaskDescriptors(Registry registry, String dataflowPath) throws RegistryException {
    MultiDataGet<DataflowTaskDescriptor> multiGet = registry.createMultiDataGet(DataflowTaskDescriptor.class);
    multiGet.getChildren(dataflowPath + "/tasks/task-list");
    multiGet.shutdown();
    multiGet.waitForAllGet(30000);
    return multiGet.getResults();
  }
  
  static public ActivityRegistry getActivityRegistry(Registry registry, String dataflowPath) throws RegistryException {
    return new ActivityRegistry(registry, dataflowPath + "/" + ACTIVITIES_PATH) ;
  }
  
  static public List<DataflowTaskReport> asyncGetDataflowTaskReports(Registry registry, String dataflowPath) throws RegistryException {
      MultiDataGet<DataflowTaskReport> multiGet = registry.createMultiDataGet(DataflowTaskReport.class);
      String taskListPath = dataflowPath + "/tasks/task-list";
      List<String> taskIds = null;
      try {
        taskIds = registry.getChildren(taskListPath) ;
      } catch(RegistryException ex) {
        if(ex.getErrorCode() == ErrorCode.NoNode) return new ArrayList<>();
        throw ex;
      }
      for(String selTaskId : taskIds) {
        multiGet.get(taskListPath + "/" + selTaskId + "/report");
      }
      multiGet.shutdown();
      multiGet.waitForAllGet(5000);
      return multiGet.getResults();
  }
  
  static public List<DataflowTaskRuntimeReport> getDataflowTaskRuntimeReports(Registry registry, String dataflowPath) throws RegistryException {
    String taskListPath = dataflowPath + "/tasks/task-list";
    
    List<String> taskIds = null;
    try {
      taskIds = registry.getChildren(taskListPath) ;
      List<DataflowTaskRuntimeReport> holder = new ArrayList<>();
      for(String selTaskId : taskIds) {
        holder.add(new DataflowTaskRuntimeReport(registry, taskListPath + "/" + selTaskId));
      }
      return holder;
    } catch(RegistryException ex) {
      if(ex.getErrorCode() == ErrorCode.NoNode) return new ArrayList<>();
      throw ex ;
    }
  }
  
  static public List<DataflowWorkerRuntimeReport> getAllDataflowWorkerRuntimeReports(Registry registry, String dataflowPath) throws RegistryException {
    String workerListPath = dataflowPath + "/workers/all";
    return getDataflowWorkerRuntimeReports(registry, workerListPath);
  }
  
  static public List<DataflowWorkerRuntimeReport> getActiveDataflowWorkerRuntimeReports(Registry registry, String dataflowPath) throws RegistryException {
    String workerListPath = dataflowPath + "/workers/active";
    return getDataflowWorkerRuntimeReports(registry, workerListPath);
  }
  
  static public List<DataflowWorkerRuntimeReport> getDataflowWorkerRuntimeReports(Registry registry, String workerListPath) throws RegistryException {
    try {
      List<String> workerIds = registry.getChildren(workerListPath) ;
      List<DataflowWorkerRuntimeReport> holder = new ArrayList<>();
      for(String selWorkerId : workerIds) {
        holder.add(new DataflowWorkerRuntimeReport(registry, workerListPath + "/" + selWorkerId));
      }
      return holder;
    } catch(RegistryException ex) {
      if(ex.getErrorCode() == ErrorCode.NoNode) return new ArrayList<>();
      throw ex;
    }
  }

  static public List<MetricRegistrySnapshot> getMetrics(Registry registry, String dataflowPath) throws RegistryException {
    return registry.getChildrenAs(dataflowPath + "/metrics", MetricRegistrySnapshot.class) ;
  }
  
  public class ConfigurationRegistry {
    private Node configurationNode ;
    private Node logNode ;
    
    public ConfigurationRegistry(String dataflowPath) throws RegistryException {
      this.configurationNode = registry.get(dataflowPath + "/configuration") ;
      this.logNode = configurationNode.getChild("log") ;
    }
    
    public void initRegistry(DataflowDescriptor dataflowDescriptor) throws RegistryException {
      logNode.createIfNotExists();
    }
    
    public String getLogPath() { return logNode.getPath() ;}
    
    public Map<String, String> getLog() throws RegistryException {
      byte[] data = logNode.getData();
      if(data == null) return new HashMap<String, String>();
      TypeReference<Map<String, String>> typeRef = new TypeReference<Map<String, String>>() {};
      return JSONSerializer.INSTANCE.fromBytes(data, typeRef ) ;
    }
    
    public void setLog(Map<String, String> conf) throws RegistryException {
      logNode.setData(conf);
    }
  }
}