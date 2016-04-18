package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.task.TaskStatus;

public class DataStreamOperatorReportWithStatus {
  private TaskStatus status ;
  private DataStreamOperatorReport report;

  public DataStreamOperatorReportWithStatus() {}
  
  public DataStreamOperatorReportWithStatus(Registry registry, String taskPath) throws RegistryException {
    this.status = registry.getDataAs(taskPath + "/status", TaskStatus.class);
    this.report = registry.getDataAs(taskPath + "/report", DataStreamOperatorReport.class);
  }

  public TaskStatus getStatus() { return status; }
  public void setStatus(TaskStatus status) { this.status = status; }

  public DataStreamOperatorReport getReport() { return report; }
  public void setReport(DataStreamOperatorReport report) { this.report = report; }
}
