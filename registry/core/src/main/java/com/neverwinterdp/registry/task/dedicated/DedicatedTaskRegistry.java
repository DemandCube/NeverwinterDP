package com.neverwinterdp.registry.task.dedicated;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.neverwinterdp.registry.BatchOperations;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.Transaction;
import com.neverwinterdp.registry.lock.Lock;
import com.neverwinterdp.registry.notification.Notifier;
import com.neverwinterdp.registry.task.TaskExecutorDescriptor;
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
  
  final static public String EXECUTIONS_TASK_FINISHED_PATH        = EXECUTIONS_PATH + "/task-finished";
  final static public String EXECUTIONS_TASK_AVAILABLE_PATH       = EXECUTIONS_PATH + "/task-available";
  
  final static public String EXECUTIONS_EXECUTORS_PATH            = EXECUTIONS_PATH + "/executors";
  final static public String EXECUTIONS_EXECUTORS_HEARTBEAT_PATH  = EXECUTIONS_PATH + "/executors/heartbeat";
  final static public String EXECUTIONS_EXECUTORS_ALL_PATH        = EXECUTIONS_PATH + "/executors/all";
  final static public String EXECUTIONS_EXECUTORS_ACTIVE_PATH     = EXECUTIONS_PATH + "/executors/active";
  final static public String EXECUTIONS_EXECUTORS_IDLE_PATH       = EXECUTIONS_PATH + "/executors/idle";
  final static public String EXECUTIONS_EXECUTORS_HISTORY_PATH    = EXECUTIONS_PATH + "/executors/history";
  
  final static public String EXECUTIONS_LOCK_PATH                 = EXECUTIONS_PATH + "/lock";
  
  final static public String TASK_STATUS_PATH                     = "status";
  
  final static public String NOTIFICATIONS_PATH                   = "notifications";

  private Registry registry ;
  private String   path ;
  private Class<T> taskDescriptorType;
  
  private Node     tasksRootNode ;
  private Node     tasksListNode ;
  private Node     executionsNode;
  private Node     taskAvailableNode;
  private Node     taskFinishedNode;
  
  private Node     executorsNode;
  private Node     executorsAllNode;
  private Node     executorsActiveNode;
  private Node     executorsIdleNode;
  private Node     executorsHistoryNode;
  private Node     executorsHeartbeatNode;
  
  private Node     tasksLockNode ; 
  
  private Notifier taskExecutionNotifier ;
  private Notifier taskCoordinationNotifier ;
  
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
    taskAvailableNode = tasksRootNode.getDescendant(EXECUTIONS_TASK_AVAILABLE_PATH);
    taskFinishedNode = tasksRootNode.getDescendant(EXECUTIONS_TASK_FINISHED_PATH);
    
    executorsNode = tasksRootNode.getDescendant(EXECUTIONS_EXECUTORS_PATH);
    executorsAllNode = tasksRootNode.getDescendant(EXECUTIONS_EXECUTORS_ALL_PATH);
    executorsActiveNode = tasksRootNode.getDescendant(EXECUTIONS_EXECUTORS_ACTIVE_PATH);
    executorsIdleNode = tasksRootNode.getDescendant(EXECUTIONS_EXECUTORS_IDLE_PATH);
    executorsHistoryNode = tasksRootNode.getDescendant(EXECUTIONS_EXECUTORS_HISTORY_PATH);
    executorsHeartbeatNode = tasksRootNode.getDescendant(EXECUTIONS_EXECUTORS_HEARTBEAT_PATH);
    
    tasksLockNode = tasksRootNode.getDescendant(EXECUTIONS_LOCK_PATH);
    
    taskExecutionNotifier = new Notifier(registry, path + "/" + NOTIFICATIONS_PATH, "task-execution");
    taskCoordinationNotifier = new Notifier(registry, path + "/" + NOTIFICATIONS_PATH, "task-coordination");
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
    transaction.create(taskAvailableNode, null, NodeCreateMode.PERSISTENT);
    transaction.create(taskFinishedNode,  null, NodeCreateMode.PERSISTENT);
    
    transaction.create(executorsNode, null, NodeCreateMode.PERSISTENT);
    transaction.create(executorsAllNode,  null, NodeCreateMode.PERSISTENT);
    transaction.create(executorsActiveNode,  null, NodeCreateMode.PERSISTENT);
    transaction.create(executorsIdleNode,  null, NodeCreateMode.PERSISTENT);
    transaction.create(executorsHistoryNode,  null, NodeCreateMode.PERSISTENT);
    transaction.create(executorsHeartbeatNode,  null, NodeCreateMode.PERSISTENT);
    
    transaction.create(tasksLockNode, null, NodeCreateMode.PERSISTENT);
    
    transaction.create(path + "/" + NOTIFICATIONS_PATH, null, NodeCreateMode.PERSISTENT);
    taskExecutionNotifier.initRegistry(transaction);
    taskCoordinationNotifier.initRegistry(transaction);
  }
  
  public Registry getRegistry() { return registry; }

  public String getPath() { return path; }
  
  public Node getTasksRootNode() { return tasksRootNode; }
  
  public Node getTasksListNode() { return tasksListNode; }
  
  public Node getTaskAvailableNode() { return taskAvailableNode; }

  public Node getTaskFinishedNode() { return taskFinishedNode; }
  
  public Node getExecutorsHeartbeatNode() { return executorsHeartbeatNode; }
  
  public Notifier getTaskCoordinationNotifier() { return taskCoordinationNotifier; }
  
  public T getTaskDescriptor(String taskId) throws RegistryException {
    return tasksListNode.getChild(taskId).getDataAs(taskDescriptorType) ;
  }
  
  public TaskStatus getTaskStatus(String taskId) throws RegistryException {
    return tasksListNode.getChild(taskId).getChild(TASK_STATUS_PATH).getDataAs(TaskStatus.class) ;
  }
  
  public void offer(String taskId, T taskDescriptor) throws RegistryException {
    Transaction transaction = registry.getTransaction() ;
    transaction.createChild(tasksListNode, taskId, taskDescriptor, NodeCreateMode.PERSISTENT);
    transaction.createDescendant(tasksListNode, taskId + "/" + TASK_STATUS_PATH, TaskStatus.INIT, NodeCreateMode.PERSISTENT);
    transaction.createChild(taskAvailableNode,  taskId + "-", NodeCreateMode.PERSISTENT_SEQUENTIAL);
    transaction.commit();
  }
  
  public List<DedicatedTaskContext<T>> take(final TaskExecutorDescriptor executor, final int reqNumOfTasks) throws RegistryException {
    BatchOperations<List<DedicatedTaskContext<T>>> takeOp = new BatchOperations<List<DedicatedTaskContext<T>>>() {
      @Override
      public List<DedicatedTaskContext<T>> execute(Registry registry) throws RegistryException {
        List<DedicatedTaskContext<T>> holder = new ArrayList<>();
        List<String> availableTasks = taskAvailableNode.getChildren() ;
        if(availableTasks.size() == 0) return holder ;
        Collections.sort(availableTasks, TASK_ID_SEQ_COMPARATOR);
        int numOfTasks = reqNumOfTasks < availableTasks.size() ? reqNumOfTasks : availableTasks.size();
        Transaction transaction = registry.getTransaction();
        try {
          for(int i = 0; i < numOfTasks; i++) {
            String taskIdSeq = availableTasks.get(i) ;
            String taskId = taskIdSeq.substring(0, taskIdSeq.lastIndexOf('-'));

            Node taskNode = tasksListNode.getChild(taskId) ;
            Node executorTaskNode = executorsAllNode.getDescendant(executor.getId() + "/tasks");
            transaction.setData(taskNode.getChild(TASK_STATUS_PATH), TaskStatus.PROCESSING);
            transaction.createChild(executorTaskNode, taskId, NodeCreateMode.PERSISTENT);

            transaction.deleteChild(taskAvailableNode, taskIdSeq);
            holder.add(createTaskContext(executor, taskId, null));
          }
          transaction.commit();
        } catch(Exception ex) {
          String errorMessage = "Fail to grab tasks for the executor " + executor.getId();
          StringBuilder registryDump = new StringBuilder() ;
          errorMessage += "\n" + registryDump.toString();
          taskExecutionNotifier.warn("fail-to-grab-a-task ", errorMessage, ex);
          throw ex;
        }
        return holder;
      }
    };
    try {
      Lock lock = tasksLockNode.getLock("write", "Lock to grab a task for the executor " + executor.getId()) ;
      return lock.execute(takeOp, 3, 3000);
    } catch(RegistryException ex) {
      String errorMessage = "Fail to assign the task after 3 tries";
      taskExecutionNotifier.error("fail-to-grab-a-task", errorMessage, ex);
      throw ex;
    }
  }
  
  DedicatedTaskContext<T> createTaskContext(TaskExecutorDescriptor executor, String taskId, T taskDescriptor) throws RegistryException {
    DedicatedTaskContext<T> taskContext = new DedicatedTaskContext<T>(this, executor, taskId, taskDescriptor);
    return taskContext;
  }
  
  public void suspend(final TaskExecutorDescriptor executor, final String taskId) throws RegistryException {
    BatchOperations<Boolean> suspendtOp = new BatchOperations<Boolean>() {
      @Override
      public Boolean execute(Registry registry) throws RegistryException {
        try {
          Node taskNode = tasksListNode.getChild(taskId) ;
          Transaction transaction = registry.getTransaction();
          transaction.setData(taskNode.getChild(TASK_STATUS_PATH), TaskStatus.SUSPENDED);
          
          Node executorTaskNode = executorsAllNode.getDescendant(executor.getId() + "/tasks");
          transaction.deleteChild(executorTaskNode, taskId);
          
          transaction.createChild(taskAvailableNode, taskId + "-", NodeCreateMode.PERSISTENT_SEQUENTIAL) ;
          transaction.commit();
          return true;
        } catch(RegistryException ex) {
          String errorMessage = "Fail to suspend the task " + taskId;
          taskExecutionNotifier.warn("fail-to-suspend-dataflow-task", errorMessage, ex);
          throw ex ;
        }
      }
    };
    try {
      Lock lock = tasksLockNode.getLock("write", "Lock to move the task " + taskId + " to suspend by " + executor.getId()) ;
      lock.execute(suspendtOp, 3, 5000);
    } catch(RegistryException ex) {
      String errorMessage = "Fail to suspend the task " + taskId;
      taskExecutionNotifier.error("fail-to-suspend-task", errorMessage, ex);
      throw ex;
    }
  }
  
  public void finish(final TaskExecutorDescriptor executor, final String taskId, final TaskStatus status) throws RegistryException {
    BatchOperations<Boolean> suspendtOp = new BatchOperations<Boolean>() {
      @Override
      public Boolean execute(Registry registry) throws RegistryException {
        try {
          Node taskNode = tasksListNode.getChild(taskId) ;
          Transaction transaction = registry.getTransaction();
          transaction.setData(taskNode.getChild(TASK_STATUS_PATH), status);
          
          Node executorTaskNode = executorsAllNode.getDescendant(executor.getId() + "/tasks");
          transaction.deleteChild(executorTaskNode, taskId);
          
          transaction.createChild(taskFinishedNode, taskId, NodeCreateMode.PERSISTENT) ;
          transaction.commit();
          return true;
        } catch(RegistryException ex) {
          String errorMessage = "Fail to suspend the task " + taskId;
          taskExecutionNotifier.warn("fail-to-suspend-dataflow-task", errorMessage, ex);
          throw ex ;
        }
      }
    };
    try {
      Lock lock = tasksLockNode.getLock("write", "Lock to move the task " + taskId + " to suspend by " + executor.getId()) ;
      lock.execute(suspendtOp, 3, 5000);
    } catch(RegistryException ex) {
      String errorMessage = "Fail to suspend the task " + taskId;
      taskExecutionNotifier.error("fail-to-suspend-task", errorMessage, ex);
      throw ex;
    }
  }
  
  public void addTaskExecutor(final TaskExecutorDescriptor executor) throws RegistryException {
    BatchOperations<Boolean> op = new BatchOperations<Boolean>() {
      @Override
      public Boolean execute(Registry registry) throws RegistryException {
        Transaction transaction = registry.getTransaction();
        String executorId = executor.getId();
        transaction.createChild(executorsAllNode, executorId, executor, NodeCreateMode.PERSISTENT);
        transaction.createDescendant(executorsAllNode, executorId + "/tasks", NodeCreateMode.PERSISTENT);
        transaction.createDescendant(executorsHeartbeatNode, executorId, NodeCreateMode.EPHEMERAL);
        transaction.commit();
        return true;
      }
    };
    execute(executor, op);
  }
  
  public void idleTaskExecutor(final TaskExecutorDescriptor executor) throws RegistryException {
    if(executor.getStatus() == TaskExecutorDescriptor.TasExecutorStatus.IDLE) return;
    BatchOperations<Boolean> op = new BatchOperations<Boolean>() {
      @Override
      public Boolean execute(Registry registry) throws RegistryException {
        Transaction transaction = registry.getTransaction();
        String executorId = executor.getId();
        if(executor.getStatus() == TaskExecutorDescriptor.TasExecutorStatus.ACTIVE) {
          transaction.deleteDescendant(executorsActiveNode, executorId);
        }
        transaction.createDescendant(executorsIdleNode, executorId, NodeCreateMode.PERSISTENT);
        executor.setStatus(TaskExecutorDescriptor.TasExecutorStatus.IDLE);
        transaction.setData(executorsAllNode.getChild(executorId), executor);
        transaction.commit();
        return true;
      }
    };
    execute(executor, op);
  }
  
  public void activeTaskExecutor(final TaskExecutorDescriptor executor) throws RegistryException {
    if(executor.getStatus() == TaskExecutorDescriptor.TasExecutorStatus.ACTIVE) return;
    BatchOperations<Boolean> op = new BatchOperations<Boolean>() {
      @Override
      public Boolean execute(Registry registry) throws RegistryException {
        Transaction transaction = registry.getTransaction();
        String executorId = executor.getId();
        if(executor.getStatus() == TaskExecutorDescriptor.TasExecutorStatus.IDLE) {
          transaction.deleteDescendant(executorsIdleNode, executorId);
        }
        transaction.createDescendant(executorsActiveNode, executorId, NodeCreateMode.PERSISTENT);
        executor.setStatus(TaskExecutorDescriptor.TasExecutorStatus.ACTIVE);
        transaction.setData(executorsAllNode.getChild(executorId), executor);
        transaction.commit();
        return true;
      }
    };
    execute(executor, op);
  }
  
  public void historyTaskExecutor(final TaskExecutorDescriptor executor) throws RegistryException {
    BatchOperations<Boolean> op = new BatchOperations<Boolean>() {
      @Override
      public Boolean execute(Registry registry) throws RegistryException {
        Transaction transaction = registry.getTransaction();
        String executorId = executor.getId();
        if(executor.getStatus() == TaskExecutorDescriptor.TasExecutorStatus.IDLE) {
          transaction.deleteDescendant(executorsIdleNode, executorId);
        } else if(executor.getStatus() == TaskExecutorDescriptor.TasExecutorStatus.ACTIVE) {
          transaction.deleteDescendant(executorsActiveNode, executorId);
        } 
        transaction.createDescendant(executorsHistoryNode, executorId, NodeCreateMode.PERSISTENT);
        executor.setStatus(TaskExecutorDescriptor.TasExecutorStatus.TERMINATED);
        transaction.setData(executorsAllNode.getChild(executorId), executor);
        transaction.commit();
        return true;
      }
    };
    execute(executor, op);
  }
  
  void execute(final TaskExecutorDescriptor executor, BatchOperations<Boolean> op) throws RegistryException {
    try {
      Lock lock = tasksLockNode.getLock("write", "Lock to update the executor " + executor.getId()) ;
      lock.execute(op, 3, 3000);
    } catch(RegistryException ex) {
      String errorMessage = "Fail to update an executor after 3 tries";
      taskExecutionNotifier.error("fail-to-update-executor", errorMessage, ex);
      throw ex;
    }
  }
}