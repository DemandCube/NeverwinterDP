package com.neverwinterdp.registry.task.dedicated;

abstract public class TaskSlotExecutor<T> {
  private DedicatedTaskContext<T> taskContext;
  
  private long tickTime;
  private long tickTimeout;
  
  long lastInterrupt = System.currentTimeMillis();
  
  public TaskSlotExecutor(DedicatedTaskContext<T> taskContext) {
    this.taskContext        = taskContext;
  }

  public DedicatedTaskContext<T> getTaskContext() { return taskContext ; }
  
  public void setTickTime(long time) { tickTime = time; }
  
  public void setTickTimeout(long time) { tickTimeout = time; }
  
  public boolean isInterrupted() { return tickTime > tickTimeout ; }
  
  public void onInit() throws Exception {
  }
  
  public void onPreExecuteSlot() throws Exception {
  }
  
  abstract public long executeSlot() throws Exception;

  public void onPostExecuteSlot() throws Exception {
  }
  
  abstract public void onEvent(TaskExecutorEvent event) throws Exception ;
  
  public void onShutdown() throws Exception {
  }
}
