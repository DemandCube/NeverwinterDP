package com.neverwinterdp.registry.task.switchable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.notification.Notifier;
import com.neverwinterdp.registry.task.TaskTransactionId;

class SwitchableTaskWatcher<T> {
  private AddRemoveNodeChildrenWatcher<T> availableTaskWatcher;
  private AddRemoveNodeChildrenWatcher<T> assignedTaskWatcher;
  private AddRemoveNodeChildrenWatcher<T> finishedTaskWatcher;
  
  private SwitchableTaskRegistry<T> taskRegistry ;
  private List<SwitchableTaskMonitor<T>> taskMonitors = new ArrayList<SwitchableTaskMonitor<T>>();
  private LinkedBlockingQueue<TaskOperation<T>> taskOperationQueue = new LinkedBlockingQueue<TaskOperation<T>>() ;
  private TaskOperationExecutor taskOperationExecutor ;
  
  public SwitchableTaskWatcher(SwitchableTaskRegistry<T> tRegistry) throws RegistryException {
    this.taskRegistry = tRegistry;
    Registry registry = tRegistry.getRegistry() ;
    availableTaskWatcher = new AddRemoveNodeChildrenWatcher<T>(registry, tRegistry.getTasksAvailableNode()) {
      @Override
      public void onAddChild(String childName) {
        enqueue(new OnAvailableTaskOperation(), childName);
      }
    };
    assignedTaskWatcher = new AddRemoveNodeChildrenWatcher<T>(registry, tRegistry.getTasksAssignedHeartbeatNode()) {
      @Override
      public void onAddChild(String taskId) {
        enqueue(new OnAssignTaskOperation(), taskId);
      }
      
      @Override
      public void onRemoveChild(String taskId) {
        enqueue(new  OnDisconnectHeartbeatTaskOperation(), taskId);
      }
    };
    finishedTaskWatcher = new AddRemoveNodeChildrenWatcher<T>(registry, tRegistry.getTasksFinishedNode()) {
      @Override
      public void onAddChild(String taskId) {
        enqueue(new OnFinishTaskOperation(), taskId);
      }
    };
    taskOperationExecutor = new TaskOperationExecutor() ;
    taskOperationExecutor.start();
  }
  
  public List<SwitchableTaskMonitor<T>> getTaskMonitors() { return this.taskMonitors; }
  
  public void addTaskMonitor(SwitchableTaskMonitor<T> monitor) {
    taskMonitors.add(monitor);
  }
  
  public void onDestroy() {
    availableTaskWatcher.setComplete();
    assignedTaskWatcher.setComplete();
    finishedTaskWatcher.setComplete();
    if(taskOperationExecutor!= null && taskOperationExecutor.isAlive()) {
      taskOperationExecutor.interrupt();
    }
  }
  
  void enqueue(TaskOperation<T> op, String childName) {
    try {
      op.init(taskRegistry.createTaskContext(childName)) ;
      taskOperationQueue.offer(op) ;
    } catch (RegistryException e) {
      Notifier notifier =  taskRegistry.getTaskCoordinationNotifier();
      try {
        notifier.error("error-enqueue-a-task-monitor-operation", "Error when enqueue a task monitor operation", e);
      } catch (RegistryException e1) {
        e1.printStackTrace();
      }
    }
  }
  
  static abstract public class TaskOperation<T> {
    protected SwitchableTaskContext<T> taskContext ;
    
    public TaskOperation<T> init(SwitchableTaskContext<T> taskContext) {
      this.taskContext = taskContext;
      return this;
    }
    
    abstract public void execute() ;
  }

  class  OnAvailableTaskOperation extends TaskOperation<T> {
    public void execute() {
      for(SwitchableTaskMonitor<T> sel : taskMonitors) {
        sel.onAvailable(taskContext);
      }
    }
  }
  
  class  OnAssignTaskOperation extends TaskOperation<T> {
    public void execute() {
      for(SwitchableTaskMonitor<T> sel : taskMonitors) {
        sel.onAssign(taskContext);
      }
    }
  }
  
  class  OnDisconnectHeartbeatTaskOperation extends TaskOperation<T> {
    public void execute() {
      boolean failTask = false ;
      TaskTransactionId id = taskContext.getTaskTransactionId();
      try {
        if(taskRegistry.getTasksAssignedNode().getChild(id.getTaskTransactionId()).exists()) {
          failTask = true;
        }
      } catch (RegistryException e) {
        Notifier notifier = taskRegistry.getTaskExecutionNotifier();
        try {
          notifier.error("fail-to-process-heartbeat-disconnect", "Fail to process heartbeat disconnect event", e);
        } catch (RegistryException e1) {
          e1.printStackTrace();
        }
      }

      if(failTask) {
        for(SwitchableTaskMonitor<T> sel : taskMonitors) {
          sel.onFail(taskContext);
        }
      }
    }
  }
  
  class  OnFinishTaskOperation extends TaskOperation<T> {
    public void execute() {
      for(SwitchableTaskMonitor<T> sel : taskMonitors) {
        sel.onFinish(taskContext);
      }
    }
  }
  
  class TaskOperationExecutor extends Thread {
    public void run() {
      try {
        while(true) {
          TaskOperation<T> taskOperation = taskOperationQueue.take();
          taskOperation.execute();
        }
      } catch (InterruptedException e) {
      }
    }
  }
}