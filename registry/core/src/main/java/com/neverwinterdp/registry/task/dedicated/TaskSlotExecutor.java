package com.neverwinterdp.registry.task.dedicated;

abstract public class TaskSlotExecutor<T> {
  private DedicatedTaskContext<T> taskContext;

  public TaskSlotExecutor(DedicatedTaskContext<T> taskContext) {
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
