package com.neverwinterdp.registry.task.switchable;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.task.TaskStatus;
import com.neverwinterdp.registry.task.TaskTransactionId;

public class SwitchableTaskContext<T> {
  private TaskTransactionId taskTransactionID;
  private T                 taskDescriptor;
  private TaskStatus        taskStatus;
  private SwitchableTaskRegistry<T>   taskRegistry;

  public SwitchableTaskContext(SwitchableTaskRegistry<T> taskRegistry, String taskTransactionId, T taskDescriptor) {
    this(taskRegistry, new TaskTransactionId(taskTransactionId), taskDescriptor) ;
  }
  
  public SwitchableTaskContext(SwitchableTaskRegistry<T> taskRegistry, TaskTransactionId taskTransactionId, T taskDescriptor) {
    this.taskRegistry = taskRegistry;
    this.taskTransactionID = taskTransactionId;
    this.taskDescriptor = taskDescriptor;
  }

  public TaskTransactionId getTaskTransactionId() { return taskTransactionID; }

  public SwitchableTaskRegistry<T> getTaskRegistry() { return this.taskRegistry; }
  
  public T getTaskDescriptor(boolean reload) throws RegistryException { 
    if(taskDescriptor == null || reload) taskDescriptor = taskRegistry.getTaskDescriptor(taskTransactionID.getTaskId()) ;
    return taskDescriptor; 
  }

  public TaskStatus getTaskStatus(boolean reload) throws RegistryException { 
    if(taskStatus == null || reload) taskStatus = taskRegistry.getTaskStatus(taskTransactionID.getTaskId()) ;
    return taskStatus; 
  }
  
  public void suspend(String executorRef, boolean disconnectHeartbeat) throws RegistryException {
    taskRegistry.suspend(executorRef, taskTransactionID, disconnectHeartbeat);
  }
}
