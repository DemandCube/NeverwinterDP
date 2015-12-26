package com.neverwinterdp.registry.task.dedicated;

abstract public class TaskSlotExecutor<T> {
  private DedicatedTaskContext<T> taskContext;
  private boolean                 interrupt = false;

  public TaskSlotExecutor(DedicatedTaskContext<T> taskContext) {
    this.taskContext        = taskContext;
  }

  public DedicatedTaskContext<T> getTaskContext() { return taskContext ; }
  
  public boolean isInterrupted() { return this.interrupt ; }
  
  public void interrupt() { interrupt = true; }
  
  public void clearInterrupt() { interrupt = false; }
  
  public void onInit() throws Exception {
  }
  
  public void onPreExecuteSlot() throws Exception {
  }
  
  abstract public void executeSlot() throws Exception;

  public void onPostExecuteSlot() throws Exception {
  }
  
  abstract public void onEvent(TaskExecutorEvent event) throws Exception ;
  
  public void onShutdown() throws Exception {
  }
}
