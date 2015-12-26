package com.neverwinterdp.registry.task.dedicated;

public class TaskExecutorEvent {
  private String name ;
  
  public TaskExecutorEvent(String name) {
    this.name = name;
  }
  
  public String getName() { return this.name; }
}
