package com.neverwinterdp.registry.task.switchable;

import javax.annotation.PreDestroy;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.task.TaskTransactionId;

public class SwitchableTaskService<T>{
  private SwitchableTaskRegistry<T> taskRegistry ;
  private SwitchableTaskWatcher<T>  taskWatcher ;

  public SwitchableTaskService() { }
  
  public SwitchableTaskService(SwitchableTaskRegistry<T> taskRegistry) throws RegistryException {
    init(taskRegistry) ;
  }
  
  public SwitchableTaskService(Registry registry, String path, Class<T> taskDescriptorType) throws RegistryException {
    init(registry, path, taskDescriptorType) ;
  }
  
  protected void init(Registry registry, String path, Class<T> taskDescriptorType) throws RegistryException {
    init(new SwitchableTaskRegistry<T>(registry, path, taskDescriptorType));
  }
  
  protected void init(SwitchableTaskRegistry<T> taskReg) throws RegistryException {
    taskRegistry = taskReg;
    taskReg.initRegistry();
    taskWatcher = new SwitchableTaskWatcher<T>(taskReg) ;
  }
  
  @PreDestroy
  public void onDestroy() {
    taskWatcher.onDestroy();
  }
  
  public SwitchableTaskRegistry<T> getTaskRegistry() { return taskRegistry; }

  
  public void addTaskMonitor(SwitchableTaskMonitor<T> monitor) {
    taskWatcher.addTaskMonitor(monitor);
  }
  
  public void offer(String taskId, T taskDescriptor) throws RegistryException {
    taskRegistry.offer(taskId, taskDescriptor);
  }
  
  public SwitchableTaskContext<T> take(final String executorRefPath) throws RegistryException {
    return taskRegistry.take(executorRefPath);
  }
  
  public void suspend(final String executorRef, TaskTransactionId id) throws RegistryException {
    taskRegistry.suspend(executorRef, id);
  }
  
  public void finish(final String executorRef, TaskTransactionId id) throws RegistryException {
    taskRegistry.finish(executorRef, id);
  }
}