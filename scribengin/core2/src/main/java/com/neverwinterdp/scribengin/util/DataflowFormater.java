package com.neverwinterdp.scribengin.util;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.activity.ActivityFormatter;
import com.neverwinterdp.registry.activity.ActivityRegistry;
import com.neverwinterdp.registry.activity.ActivityStep;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.operator.OperatorTaskReport;
import com.neverwinterdp.scribengin.dataflow.operator.OperatorTaskRuntimeReport;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.worker.DataflowWorkerRuntimeReport;
import com.neverwinterdp.scribengin.dataflow.worker.TaskExecutorDescriptor;
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
  
  public DataflowFormater(DataflowRegistry dflRegistry) {
    this.registry     = dflRegistry.getRegistry();
    this.dataflowPath = dflRegistry.getDataflowPath();
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
    List<OperatorTaskRuntimeReport> reports =  DataflowRegistry.getDataflowTaskRuntimeReports(registry, dataflowPath);
    LinkedHashMap<String, List<OperatorTaskRuntimeReport>> groupByOperatorReports = new LinkedHashMap<>();
    for(int i = 0; i < reports.size(); i++) {
      OperatorTaskRuntimeReport rtReport = reports.get(i);
      String operator = rtReport.getReport().getOperatorName();
      List<OperatorTaskRuntimeReport> holder = groupByOperatorReports.get(operator);
      if(holder == null) {
        holder = new ArrayList<>();
        groupByOperatorReports.put(operator, holder);
      }
      holder.add(rtReport);
    }
    
    String[] header = {
      "Task Id", "Status", "Assigned", "AHE", "AWNM", "LAWNM", "AC", "CC", "CFC", "Last Commit Time", "Start Time", "Finish Time", "Exec Time", "Duration"
    } ;
    TabularFormater taskFt = new TabularFormater(header);
    taskFt.setTitle("Dataflow Task Info");
    taskFt.addFooter("AHE   = Assigned Has Error");
    taskFt.addFooter("AWNM  = Assigned With No Message Count");
    taskFt.addFooter("LAWNM = Last Assigned With No Message Count");
    taskFt.addFooter("AC    = Accumulate Message Commit Count");
    taskFt.addFooter("CC    = Commit Count");
    taskFt.addFooter("CFC   = Commit Fail Count");
    for(Map.Entry<String, List<OperatorTaskRuntimeReport>> entry : groupByOperatorReports.entrySet()) {
      String operator = entry.getKey();
      List<OperatorTaskRuntimeReport> operatorReports = entry.getValue();
      taskFt.addRow(operator, "", "", "", "", "", "", "", "", "", "", "", "", "");
      for(int i = 0; i < operatorReports.size(); i++) {
        OperatorTaskRuntimeReport rtReport = operatorReports.get(i);
        OperatorTaskReport report = rtReport.getReport();
        taskFt.addRow(
            "  " + report.getTaskId(), 
            rtReport.getStatus(), 
            report.getAssignedCount(),
            report.getAssignedHasErrorCount(),
            report.getAssignedWithNoMessageProcess(),
            report.getLastAssignedWithNoMessageProcess(),
            report.getAccCommitProcessCount(),
            report.getCommitCount(),
            report.getCommitFailCount(),
            DateUtil.asCompactDateTime(report.getLastCommitTime()),
            DateUtil.asCompactDateTime(report.getStartTime()),
            DateUtil.asCompactDateTime(report.getFinishTime()),
            report.getAccRuntime() + "ms",
            report.durationTime() + "ms"
        );
      }
    }
    return taskFt.getFormattedText();
  }
  
  public String getActivitiesInfo() throws RegistryException {
    StringBuilder b = new StringBuilder() ;
    b.append("\nActivities:").append("\n");
    b.append("  Active Activities:");
    ActivityRegistry activityRegistry = DataflowRegistry.getMasterActivityRegistry(registry, dataflowPath) ;
    List<Activity> ActiveActivities = activityRegistry.getActiveActivities();
    for(Activity activity : ActiveActivities) {
      List<ActivityStep> steps = activityRegistry.getActivitySteps(activity);
      ActivityFormatter activityFormatter = new ActivityFormatter(activity, steps, true);
      b.append(activityFormatter.format("    "));
      b.append("\n");
    }
    
    b.append("  History Activities:\n");
    List<Activity> historyActivities = activityRegistry.getHistoryActivities();
    for(Activity activity : historyActivities) {
      List<ActivityStep> steps = activityRegistry.getActivitySteps(activity);
      ActivityFormatter activityFormatter = new ActivityFormatter(activity, steps, true);
      b.append(activityFormatter.format("    "));
    }
    return b.toString();
  }
  
  public String getDataflowWorkerInfo() throws RegistryException {
    List<DataflowWorkerRuntimeReport> reports =  DataflowRegistry.getAllDataflowWorkerRuntimeReports(registry, dataflowPath);
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
      for(TaskExecutorDescriptor selExecutor : rtReport.getExecutors()) {
        taskFt.addRow(
          "", "",
          selExecutor.getId(), selExecutor.getStatus(), StringUtil.join(selExecutor.getAssignedTaskIds(), ",")
        );
      }
    }
    return taskFt.getFormattedText();
  }
}
