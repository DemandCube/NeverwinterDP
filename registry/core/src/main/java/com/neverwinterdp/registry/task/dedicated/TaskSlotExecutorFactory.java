package com.neverwinterdp.registry.task.dedicated;

public interface TaskSlotExecutorFactory<T> {
  public TaskSlotExecutor<T> create(DedicatedTaskContext<T> context) throws Exception ;
}
