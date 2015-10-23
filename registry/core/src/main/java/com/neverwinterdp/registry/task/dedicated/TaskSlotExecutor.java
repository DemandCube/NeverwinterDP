package com.neverwinterdp.registry.task.dedicated;

import com.neverwinterdp.registry.task.TaskExecutorDescriptor;

abstract public class TaskSlotExecutor<T> {
  private TaskExecutorDescriptor  executorDescriptor;
  private DedicatedTaskContext<T> taskContext;

  public TaskSlotExecutor(TaskExecutorDescriptor  executor, DedicatedTaskContext<T> taskContext) {
    this.executorDescriptor = executor;
    this.taskContext        = taskContext;
  }

  public DedicatedTaskContext<T> getTaskContext() { return taskContext ; }
  
  public void onInit() throws Exception {
  }
  
  public void onPreExecuteSlot() throws Exception {
  }
  
  abstract public void executeSlot() throws Exception;

  public void onPostExecuteSlot() throws Exception {
  }
}
