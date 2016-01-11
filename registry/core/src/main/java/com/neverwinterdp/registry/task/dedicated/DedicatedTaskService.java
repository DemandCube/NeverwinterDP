package com.neverwinterdp.registry.task.dedicated;

import java.util.List;

import javax.annotation.PreDestroy;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.task.TaskExecutorDescriptor;
import com.neverwinterdp.registry.task.TaskStatus;

final public class DedicatedTaskService<T> {
  private DedicatedTaskRegistry<T>   taskRegistry;
  private TaskExecutorService<T>     taskExecutorService;
  private TaskSlotExecutorFactory<T> taskSlotExecutorFactory;

  public DedicatedTaskService(DedicatedTaskRegistry<T> taskRegistry, TaskSlotExecutorFactory<T> taskSlotExecutorFactory) throws RegistryException {
    this.taskRegistry = taskRegistry;
    this.taskSlotExecutorFactory = taskSlotExecutorFactory;
    taskExecutorService = new TaskExecutorService<T>();
  }
  
  @PreDestroy
  public void onDestroy() throws InterruptedException {
    taskExecutorService.shutdown();
  } 
  
  public void simulateKill() {
    taskExecutorService.simulateKill();
  }
  
  public DedicatedTaskRegistry<T> getTaskRegistry() { return this.taskRegistry; }
  
  public TaskSlotExecutorFactory<T> getTaskSlotExecutorFactory() { return taskSlotExecutorFactory; }
  
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
  
  //================== Executor =======================================================
  
  public TaskExecutorService<T> getTaskExecutorService() { return this.taskExecutorService; }
  
  public void addExecutor(TaskExecutorDescriptor executorDescriptor, int taskSlots) throws Exception {
    taskRegistry.addTaskExecutor(executorDescriptor);
    TaskExecutor<T> executor = new TaskExecutor<T>(executorDescriptor.getId(), this, taskSlots) ;
    taskExecutorService.add(executor);
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
