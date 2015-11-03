package com.neverwinterdp.scribengin.dataflow.registry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.task.TaskExecutorDescriptor;
import com.neverwinterdp.registry.task.TaskStatus;
import com.neverwinterdp.registry.task.dedicated.DedicatedTaskContext;
import com.neverwinterdp.registry.task.dedicated.DedicatedTaskRegistry;
import com.neverwinterdp.scribengin.dataflow.operator.OperatorTaskConfig;
import com.neverwinterdp.scribengin.dataflow.operator.OperatorTaskReport;
import com.neverwinterdp.scribengin.dataflow.operator.OperatorTaskRuntimeReport;

public class DataflowTaskRegistry extends DedicatedTaskRegistry<OperatorTaskConfig> {
  private String dataflowPath ;
  
  public DataflowTaskRegistry(Registry registry, String dataflowPath) throws RegistryException {
    init(registry, dataflowPath + "/tasks", OperatorTaskConfig.class) ;
    this.dataflowPath = dataflowPath;
  }

  public void offer(OperatorTaskConfig taskConfig) throws RegistryException {
    super.offer(taskConfig.getTaskId(), taskConfig);
    create(taskConfig, new OperatorTaskReport(taskConfig.getTaskId(), taskConfig.getOperatorName()));
  }
  
  public OperatorTaskReport getTaskReport(OperatorTaskConfig descriptor) throws RegistryException {
    Node taskNode = getTasksListNode().getChild(descriptor.getTaskId());
    return getRegistry().getDataAs(taskNode.getPath() + "/report", OperatorTaskReport.class) ;
  }
  
  public List<OperatorTaskReport> getTaskReports(List<OperatorTaskConfig> tConfigs) throws RegistryException {
    List<String> reportPaths = new ArrayList<String>();
    for(int i = 0; i < tConfigs.size(); i++) {
      OperatorTaskConfig descriptor = tConfigs.get(i);
      Node taskNode = getTasksListNode().getChild(descriptor.getTaskId());
      reportPaths.add(taskNode.getPath() + "/report") ;
    }
    return getRegistry().getDataAs(reportPaths, OperatorTaskReport.class) ;
  }
  
  public void save(OperatorTaskConfig tConfig, OperatorTaskReport report) throws RegistryException {
    Node  reportNode = getTasksListNode().getChild(tConfig.getTaskId()).getChild("report");
    reportNode.setData(report);
  }
  
  public void create(OperatorTaskConfig taskConfig, OperatorTaskReport report) throws RegistryException {
    Node taskNode = getTasksListNode().getChild(taskConfig.getTaskId());
    taskNode.createChild("report", report, NodeCreateMode.PERSISTENT);
  }
  
  public void suspend(DedicatedTaskContext<OperatorTaskConfig> context) throws RegistryException {
    suspend(context.getTaskExecutorDescriptor(), context.getTaskId()) ;
  }
  
  public void suspend(TaskExecutorDescriptor executor, DedicatedTaskContext<OperatorTaskConfig> context) throws RegistryException {
    suspend(executor, context.getTaskId()) ;
  }
  
  public void finish(DedicatedTaskContext<OperatorTaskConfig> context, TaskStatus taskStatus) throws RegistryException {
    finish(context.getTaskExecutorDescriptor(), context.getTaskId(), taskStatus) ;
  }

  public List<String> getAllExecutorIds() throws RegistryException {
    List<String> executorIds = getExecutorsAllNode().getChildren();
    Collections.sort(executorIds);
    return executorIds;
  }
  
  public List<OperatorTaskRuntimeReport> getDataflowTaskRuntimeReportsByExecutorId(String executorId) throws RegistryException {
    List<String> taskIds = getExecutorsAllNode().getChild(executorId).getChild("tasks").getChildren() ;
    return getDataflowTaskRuntimeReports(taskIds) ;
  }
  
  
  public List<OperatorTaskRuntimeReport> getDataflowTaskRuntimeReports() throws RegistryException {
    List<String> taskIds = getTasksListNode().getChildren() ;
    return getDataflowTaskRuntimeReports(taskIds);
  }

  List<OperatorTaskRuntimeReport> getDataflowTaskRuntimeReports(List<String> taskIds) throws RegistryException {
    String taskListPath = getTasksListNode().getPath();
    List<OperatorTaskRuntimeReport> holder = new ArrayList<>();
    for(String selTaskId : taskIds) {
      holder.add(new OperatorTaskRuntimeReport(getRegistry(), taskListPath + "/" + selTaskId));
    }
    return holder;
  }
}
