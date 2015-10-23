package com.neverwinterdp.registry.task;

import java.util.ArrayList;
import java.util.List;

public class TaskExecutorDescriptor {
  static public enum TasExecutorStatus {  
    INIT, ACTIVE, IDLE, TERMINATED, TERMINATED_WITH_INTERRUPT, TERMINATED_WITH_ERROR  
  }
  
  private String            id;
  private String            workerRef;
  private TasExecutorStatus status          = TasExecutorStatus.INIT;
  private List<String>      assignedTaskIds = new ArrayList<>();

  public TaskExecutorDescriptor() {}
  
  public TaskExecutorDescriptor(String id, String workerRef) {
    this.id = id;
    this.workerRef = workerRef;
  }
  
  public String getId() { return id; }
  public void setId(String id) { this.id = id; }

  public String getWorkerRef() { return workerRef; }
  public void setWorkerRef(String workerRef) { this.workerRef = workerRef; }
  
  public TasExecutorStatus getStatus() { return status; }
  public void setStatus(TasExecutorStatus status) { this.status = status; }

  public List<String> getAssignedTaskIds() { return assignedTaskIds; }
  public void setAssignedTaskIds(List<String> assignedTaskIds) { this.assignedTaskIds = assignedTaskIds; }
  
  public void addAssignedTask(String taskId) {
    assignedTaskIds.add(taskId);
  }
}
