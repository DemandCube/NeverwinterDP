package com.neverwinterdp.registry.task.dedicated;

import java.util.List;

import javax.annotation.PreDestroy;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.task.TaskExecutorDescriptor;
import com.neverwinterdp.registry.task.TaskStatus;

public class DedicatedTaskService<T> {
  private DedicatedTaskRegistry<T> taskRegistry ;

  public DedicatedTaskService() { }
  
  public DedicatedTaskService(DedicatedTaskRegistry<T> taskRegistry) throws RegistryException {
    init(taskRegistry) ;
  }
  
  public DedicatedTaskService(Registry registry, String path, Class<T> taskDescriptorType) throws RegistryException {
    init(registry, path, taskDescriptorType) ;
  }
  
  protected void init(Registry registry, String path, Class<T> taskDescriptorType) throws RegistryException {
    init(new DedicatedTaskRegistry<T>(registry, path, taskDescriptorType));
  }
  
  protected void init(DedicatedTaskRegistry<T> taskReg) throws RegistryException {
    taskRegistry = taskReg;
    taskReg.initRegistry();
  }
  
  @PreDestroy
  public void onDestroy() {
  } 
  
  public DedicatedTaskRegistry<T> getTaskRegistry() { return this.taskRegistry; }
  
  public void offer(String taskId, T taskDescriptor) throws RegistryException {
    taskRegistry.offer(taskId, taskDescriptor);
  }
  
  public List<DedicatedTaskContext<T>> take(TaskExecutorDescriptor executor, int maxNumOfTasks) throws RegistryException {
    return taskRegistry.take(executor, maxNumOfTasks);
  }
  
  public void suspend(TaskExecutorDescriptor executor, String taskId) throws RegistryException {
    taskRegistry.suspend(executor, taskId);
  }
  
  public void finish(TaskExecutorDescriptor executor, String taskId, TaskStatus status) throws RegistryException {
    taskRegistry.finish(executor, taskId, status);
  }
  
  public void addExecutor(TaskExecutorDescriptor executor) throws RegistryException {
    taskRegistry.addTaskExecutor(executor);
  }
  
  public void activeExecutor(TaskExecutorDescriptor executor) throws RegistryException {
    taskRegistry.activeTaskExecutor(executor);;
  }
  
  public void idleExecutor(TaskExecutorDescriptor executor) throws RegistryException {
    taskRegistry.idleTaskExecutor(executor);
  }
  
  public void historyExecutor(TaskExecutorDescriptor executor) throws RegistryException {
    taskRegistry.historyTaskExecutor(executor);
  }
}
