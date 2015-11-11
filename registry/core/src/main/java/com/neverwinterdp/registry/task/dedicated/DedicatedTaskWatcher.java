package com.neverwinterdp.registry.task.dedicated;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;

class DedicatedTaskWatcher<T> {
  private AddRemoveNodeChildrenWatcher<T> availableTaskWatcher;
  private AddRemoveNodeChildrenWatcher<T> finishedTaskWatcher;
  private AddRemoveNodeChildrenWatcher<T> executorsWatcher;
  
  private DedicatedTaskRegistry<T>              taskRegistry;
  private List<DedicatedTaskMonitor<T>>         taskMonitors       = new ArrayList<DedicatedTaskMonitor<T>>();
  private LinkedBlockingQueue<TaskOperation<T>> taskOperationQueue = new LinkedBlockingQueue<TaskOperation<T>>();
  private TaskOperationExecutor                 taskOperationExecutor;
  
  public DedicatedTaskWatcher(DedicatedTaskRegistry<T> tRegistry) throws RegistryException {
    this.taskRegistry = tRegistry;
    Registry registry = tRegistry.getRegistry() ;
    availableTaskWatcher = new AddRemoveNodeChildrenWatcher<T>(registry, tRegistry.getTaskAvailableNode()) {
      @Override
      public void onAddChild(String childName) {
        enqueue(new OnAvailableTaskOperation(), null, childName);
      }
    };
    
    finishedTaskWatcher = new AddRemoveNodeChildrenWatcher<T>(registry, tRegistry.getTaskFinishedNode()) {
      @Override
      public void onAddChild(String taskId) {
        enqueue(new OnFinishTaskOperation(), null, taskId);
      }
    };
    
    executorsWatcher = new AddRemoveNodeChildrenWatcher<T>(registry, tRegistry.getExecutorsHeartbeatNode()) {
      @Override
      public void onAddChild(String executorId) {
        enqueue(new OnAddExecutorOperation(), executorId, null);
      }
      
      @Override
      public void onRemoveChild(String executorId) {
        enqueue(new OnLostExecutorOperation(), executorId, null);
      }
    };
    
    taskOperationExecutor = new TaskOperationExecutor() ;
    taskOperationExecutor.start();
  }
  
  public List<DedicatedTaskMonitor<T>> getTaskMonitors() { return this.taskMonitors; }
  
  public void addTaskMonitor(DedicatedTaskMonitor<T> monitor) {
    taskMonitors.add(monitor);
  }
  
  public void onDestroy() {
    availableTaskWatcher.setComplete();
    finishedTaskWatcher.setComplete();
    executorsWatcher.setComplete();
    if(taskOperationExecutor!= null && taskOperationExecutor.isAlive()) {
      taskOperationExecutor.interrupt();
    }
  }
  
  void enqueue(TaskOperation<T> op, String executorId, String taskId) {
    op.init(taskRegistry, executorId, taskId) ;
    taskOperationQueue.offer(op) ;
  }
  
  static abstract public class TaskOperation<T> {
    DedicatedTaskRegistry<T> taskRegistry ;
    String executorId;
    String taskId;
    
    public TaskOperation<T> init(DedicatedTaskRegistry<T> taskRegistry, String executorId, String taskId) {
      this.taskRegistry = taskRegistry;
      this.executorId = executorId;
      this.taskId = taskId;
      return this;
    }
    
    abstract public void execute() ;
  }

  class  OnAvailableTaskOperation extends TaskOperation<T> {
    public void execute() {
      for(DedicatedTaskMonitor<T> sel : taskMonitors) {
        sel.onAvailable(taskRegistry, taskId);
      }
    }
  }
  
  class  OnFinishTaskOperation extends TaskOperation<T> {
    public void execute() {
      for(DedicatedTaskMonitor<T> sel : taskMonitors) {
        sel.onFinish(taskRegistry, taskId);
      }
    }
  }
  
  class  OnAddExecutorOperation extends TaskOperation<T> {
    public void execute() {
      for(DedicatedTaskMonitor<T> sel : taskMonitors) {
        sel.onAddExecutor(taskRegistry, executorId);;
      }
    }
  }
  
  class  OnLostExecutorOperation extends TaskOperation<T> {
    public void execute() {
      for(DedicatedTaskMonitor<T> sel : taskMonitors) {
        sel.onLostExecutor(taskRegistry, executorId);;
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