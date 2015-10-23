package com.neverwinterdp.registry.task;

final public class TaskDescriptor {
  private String description;

  public TaskDescriptor() { }
  
  public TaskDescriptor(String desc) {
    this.description = desc;
  }
  
  public String getDescription() { return description; }
  public void setDescription(String description) { this.description = description; }
}