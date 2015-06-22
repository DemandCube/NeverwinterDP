package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.task.TaskStatus;

public class DataflowTaskRuntimeReport {
  private TaskStatus status ;
  private DataflowTaskReport report;

  public DataflowTaskRuntimeReport() {}
  
  public DataflowTaskRuntimeReport(Registry registry, String taskPath) throws RegistryException {
    this.status = registry.getDataAs(taskPath + "/status", TaskStatus.class);
    this.report = registry.getDataAs(taskPath + "/report", DataflowTaskReport.class);
  }

  public TaskStatus getStatus() { return status; }
  public void setStatus(TaskStatus status) { this.status = status; }

  public DataflowTaskReport getReport() { return report; }
  public void setReport(DataflowTaskReport report) { this.report = report; }
}
