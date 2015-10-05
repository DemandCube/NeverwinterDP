package com.neverwinterdp.scribengin.dataflow.operator;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.task.TaskStatus;

public class OperatorTaskRuntimeReport {
  private TaskStatus status ;
  private OperatorTaskReport report;

  public OperatorTaskRuntimeReport() {}
  
  public OperatorTaskRuntimeReport(Registry registry, String taskPath) throws RegistryException {
    this.status = registry.getDataAs(taskPath + "/status", TaskStatus.class);
    this.report = registry.getDataAs(taskPath + "/report", OperatorTaskReport.class);
  }

  public TaskStatus getStatus() { return status; }
  public void setStatus(TaskStatus status) { this.status = status; }

  public OperatorTaskReport getReport() { return report; }
  public void setReport(OperatorTaskReport report) { this.report = report; }
}
