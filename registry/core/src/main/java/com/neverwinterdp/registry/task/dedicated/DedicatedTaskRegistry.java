package com.neverwinterdp.registry.task.dedicated;

import java.util.Comparator;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.Transaction;
import com.neverwinterdp.registry.task.TaskStatus;

public class DedicatedTaskRegistry<T> {
  static public Comparator<String> TASK_ID_SEQ_COMPARATOR = new Comparator<String>() {
    @Override
    public int compare(String taskId_1, String taskId_2) {
      int taskIdSeq1 = Integer.parseInt(taskId_1.substring(taskId_1.lastIndexOf('-') + 1));
      int taskIdSeq2 = Integer.parseInt(taskId_2.substring(taskId_2.lastIndexOf('-') + 1));
      return taskIdSeq1 - taskIdSeq2;
    }
  };
  
  final static public String TASK_LIST_PATH                       = "task-list";
  
  final static public String EXECUTIONS_PATH                      = "executions";
  final static public String EXECUTIONS_TASK_GROUP_LIST_PATH      = EXECUTIONS_PATH + "/task-group-list";
  final static public String EXECUTIONS_TASK_GROUP_AVAILABLE_PATH = EXECUTIONS_PATH + "/task-group-available";
  final static public String EXECUTIONS_TASK_GROUP_ASSIGNED_PATH  = EXECUTIONS_PATH + "/task-group-assigned";
  final static public String EXECUTIONS_TASK_GROUP_FINISHED_PATH  = EXECUTIONS_PATH + "/task-group-finished";
  final static public String EXECUTIONS_TASK_FINISHED_PATH        = EXECUTIONS_PATH + "/task-finished";
  final static public String EXECUTIONS_LOCK_PATH                 = EXECUTIONS_PATH + "/lock";
  
  final static public String TASK_STATUS_PATH                     = "status";

  private Registry registry ;
  private String   path ;
  private Class<T> taskDescriptorType;
  private int      numOfExecutors      = 24 ;
  private int      numOfSpareExecutors = 2;
  
  private Node     tasksRootNode ;
  private Node     tasksListNode ;
  private Node     executionsNode;
  private Node     executionsTaskGroupListNode;
  private Node     executionsTaskGroupAvailableNode;
  private Node     executionsTaskGroupAssignedNode;
  private Node     executionsTaskGroupFinishedNode;
  private Node     executionsTaskFinishedNode;
  
  public DedicatedTaskRegistry() { }
  
  public DedicatedTaskRegistry(Registry registry, String path, Class<T> taskDescriptorType) throws RegistryException {
    init(registry, path, taskDescriptorType) ;
  }
  
  protected void init(Registry registry, String path, Class<T> taskDescriptorType) throws RegistryException {
    this.registry = registry;
    this.path     = path;
    this.taskDescriptorType = taskDescriptorType;
    
    tasksRootNode = registry.get(path) ;
    tasksListNode = tasksRootNode.getDescendant(TASK_LIST_PATH); 
    
    executionsNode = tasksRootNode.getChild(EXECUTIONS_PATH);
    executionsTaskGroupListNode = tasksRootNode.getDescendant(EXECUTIONS_TASK_GROUP_LIST_PATH);
    executionsTaskGroupAvailableNode = tasksRootNode.getDescendant(EXECUTIONS_TASK_GROUP_AVAILABLE_PATH);
    executionsTaskGroupAssignedNode = tasksRootNode.getDescendant(EXECUTIONS_TASK_GROUP_ASSIGNED_PATH);
    executionsTaskGroupFinishedNode = tasksRootNode.getDescendant(EXECUTIONS_TASK_GROUP_FINISHED_PATH);
    executionsTaskFinishedNode = tasksRootNode.getDescendant(EXECUTIONS_TASK_FINISHED_PATH);
  }
  
  public void initRegistry() throws RegistryException {
    Transaction transaction = registry.getTransaction();
    initRegistry(transaction);
    transaction.commit();
  }
  
  public void initRegistry(Transaction transaction) throws RegistryException {
    transaction.create(tasksRootNode, null, NodeCreateMode.PERSISTENT);
  
    transaction.create(tasksListNode, null, NodeCreateMode.PERSISTENT);
    
    transaction.create(executionsNode, null, NodeCreateMode.PERSISTENT);
    transaction.create(executionsTaskGroupListNode, null, NodeCreateMode.PERSISTENT);
    transaction.create(executionsTaskGroupAvailableNode, null, NodeCreateMode.PERSISTENT);
    transaction.create(executionsTaskGroupAssignedNode, null, NodeCreateMode.PERSISTENT);
    transaction.create(executionsTaskGroupFinishedNode, null, NodeCreateMode.PERSISTENT);
    transaction.create(executionsTaskFinishedNode, null, NodeCreateMode.PERSISTENT);
  }
  
  public Registry getRegistry() { return registry; }

  public String getPath() { return path; }
  
  public Node getTasksRootNode() { return tasksRootNode; }
  
  public Node getTasksListNode() { return tasksListNode; }

  public TaskStatus getTaskStatus(String taskId) throws RegistryException {
    return tasksListNode.getChild(taskId).getChild(TASK_STATUS_PATH).getDataAs(TaskStatus.class) ;
  }
  
  public void offer(String taskId, T taskDescriptor) throws RegistryException {
    Transaction transaction = registry.getTransaction() ;
    transaction.createChild(tasksListNode, taskId, taskDescriptor, NodeCreateMode.PERSISTENT);
    transaction.createDescendant(tasksListNode, taskId + "/" + TASK_STATUS_PATH, TaskStatus.INIT, NodeCreateMode.PERSISTENT);
    transaction.commit();
  }
}