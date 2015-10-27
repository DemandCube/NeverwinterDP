package com.neverwinterdp.registry.task.dedicated;

public interface DedicatedTaskMonitor<T> {
  public void onAddExecutor(DedicatedTaskRegistry<T>  taskRegistry, String executorId) ;
  public void onLostExecutor(DedicatedTaskRegistry<T>  taskRegistry, String executorId) ;
  
  public void onAvailable(DedicatedTaskRegistry<T>  taskRegistry, String taskId) ;
  public void onFinish(DedicatedTaskRegistry<T>  taskRegistry, String taskId) ;
}
