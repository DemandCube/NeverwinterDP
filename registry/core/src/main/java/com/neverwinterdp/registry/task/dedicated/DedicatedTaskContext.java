package com.neverwinterdp.registry.task.dedicated;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.task.TaskExecutorDescriptor;
import com.neverwinterdp.registry.task.TaskStatus;

public class DedicatedTaskContext<T> {
  private String                    taskId;
  private T                         taskDescriptor;
  private TaskStatus                taskStatus;
  private TaskExecutorDescriptor    executor;
  private DedicatedTaskRegistry<T>  taskRegistry;
  private boolean                   complete = false;

  public DedicatedTaskContext(DedicatedTaskRegistry<T> taskRegistry, TaskExecutorDescriptor executor, String taskId, T taskDescriptor) {
    this.taskRegistry   = taskRegistry;
    this.executor       = executor;
    this.taskId         = taskId;
    this.taskDescriptor = taskDescriptor;
  }

  public TaskExecutorDescriptor getTaskExecutorDescriptor() { return executor ; }
  
  public String getTaskId() { return taskId; }

  public DedicatedTaskRegistry<T> getTaskRegistry() { return this.taskRegistry; }

  public boolean isComplete() { return complete ; }
  
  public void setComplete() { complete = true; }
  
  public T getTaskDescriptor(boolean reload) throws RegistryException { 
    if(taskDescriptor == null || reload) taskDescriptor = taskRegistry.getTaskDescriptor(taskId) ;
    return taskDescriptor; 
  }

  public TaskStatus getTaskStatus(boolean reload) throws RegistryException { 
    if(taskStatus == null || reload) taskStatus = taskRegistry.getTaskStatus(taskId) ;
    return taskStatus; 
  }
}
