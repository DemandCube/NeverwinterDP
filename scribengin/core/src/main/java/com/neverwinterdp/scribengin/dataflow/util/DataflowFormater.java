package com.neverwinterdp.scribengin.dataflow.util;

import java.util.List;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskReport;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskRuntimeReport;
import com.neverwinterdp.scribengin.dataflow.DataflowWorkerRuntimeReport;
import com.neverwinterdp.scribengin.dataflow.worker.DataflowTaskExecutorDescriptor;
import com.neverwinterdp.util.text.DateUtil;
import com.neverwinterdp.util.text.StringUtil;
import com.neverwinterdp.util.text.TabularFormater;

public class DataflowFormater {
  private Registry registry;
  private String   dataflowPath;
  
  public DataflowFormater(Registry registry, String dataflowPath) {
    this.registry     = registry;
    this.dataflowPath = dataflowPath;
  }

  public String getFormattedText() throws RegistryException {
    StringBuilder b = new StringBuilder();
    b.append(getInfo()).append("\n");
    b.append(getDataflowTaskInfo()).append("\n");
    b.append(getDataflowWorkerInfo()).append("\n");
    return b.toString();
  }
  
  public String getInfo() throws RegistryException {
    Node dataflowNode = registry.get(dataflowPath);
    DataflowLifecycleStatus  status = dataflowNode.getChild("status").getDataAs(DataflowLifecycleStatus.class);
    TabularFormater infoFt = new TabularFormater("Dataflow Info", "");
    infoFt.addRow("Id", dataflowNode.getName());
    infoFt.addRow("Status", status);
    return infoFt.getFormattedText();
  }
  
  public String getDataflowTaskInfo() throws RegistryException {
    List<DataflowTaskRuntimeReport> reports =  DataflowRegistry.getDataflowTaskRuntimeReports(registry, dataflowPath);
    String[] header = {
      "Id", "Status", "Assigned", "Acc Commit", "Commit Count", "Last Commit Time", "Start Time", "Finish Time", "Exec Time", "Duration"
    } ;
    TabularFormater taskFt = new TabularFormater(header);
    taskFt.setTitle("Dataflow Task Info");
    for(int i = 0; i < reports.size(); i++) {
      DataflowTaskRuntimeReport rtReport = reports.get(i);
      DataflowTaskReport report = rtReport.getReport();
      taskFt.addRow(
          report.getTaskId(), 
          rtReport.getStatus(), 
          report.getAssignedCount(),
          report.getAccCommitProcessCount(),
          report.getCommitCount(),
          DateUtil.asCompactDateTime(report.getLastCommitTime()),
          DateUtil.asCompactDateTime(report.getStartTime()),
          DateUtil.asCompactDateTime(report.getFinishTime()),
          report.getAccRuntime() + "ms",
          report.durationTime() + "ms"
      );
    }
    return taskFt.getFormattedText();
  }
  
  public String getDataflowWorkerInfo() throws RegistryException {
    List<DataflowWorkerRuntimeReport> reports =  DataflowRegistry.getActiveDataflowWorkerRuntimeReports(registry, dataflowPath);
    String[] header = {
      "Worker", "Status", "Executor", "Executor Status", "Executor Assigned Tasks"
    } ;
    TabularFormater taskFt = new TabularFormater(header);
    taskFt.setTitle("Dataflow Worker Info");
    for(int i = 0; i < reports.size(); i++) {
      DataflowWorkerRuntimeReport rtReport = reports.get(i);
      taskFt.addRow(
        rtReport.getWorker(), 
        rtReport.getStatus(),
        "", "", ""
      );
      for(DataflowTaskExecutorDescriptor selExecutor : rtReport.getExecutors()) {
        taskFt.addRow(
          "", "",
          selExecutor.getId(), selExecutor.getStatus(), StringUtil.join(selExecutor.getAssignedTaskIds(), ",")
        );
      }
    }
    return taskFt.getFormattedText();
  }
}
