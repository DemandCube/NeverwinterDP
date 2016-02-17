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
import com.neverwinterdp.scribengin.dataflow.DataStreamOperatorDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperatorReport;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperatorReportWithStatus;

public class DataflowTaskRegistry extends DedicatedTaskRegistry<DataStreamOperatorDescriptor> {
  private String dataflowPath ;
  
  public DataflowTaskRegistry(Registry registry, String dataflowPath) throws RegistryException {
    init(registry, dataflowPath + "/tasks", DataStreamOperatorDescriptor.class) ;
    this.dataflowPath = dataflowPath;
  }

  public void offer(DataStreamOperatorDescriptor taskConfig) throws RegistryException {
    super.offer(taskConfig.getTaskId(), taskConfig);
    create(taskConfig, new DataStreamOperatorReport(taskConfig.getTaskId(), taskConfig.getOperatorName()));
  }
  
  public DataStreamOperatorReport getTaskReport(DataStreamOperatorDescriptor descriptor) throws RegistryException {
    Node taskNode = getTasksListNode().getChild(descriptor.getTaskId());
    return getRegistry().getDataAs(taskNode.getPath() + "/report", DataStreamOperatorReport.class) ;
  }
  
  public List<DataStreamOperatorReport> getTaskReports(List<DataStreamOperatorDescriptor> tConfigs) throws RegistryException {
    List<String> reportPaths = new ArrayList<String>();
    for(int i = 0; i < tConfigs.size(); i++) {
      DataStreamOperatorDescriptor descriptor = tConfigs.get(i);
      Node taskNode = getTasksListNode().getChild(descriptor.getTaskId());
      reportPaths.add(taskNode.getPath() + "/report") ;
    }
    return getRegistry().getDataAs(reportPaths, DataStreamOperatorReport.class) ;
  }
  
  public void save(DataStreamOperatorDescriptor tConfig, DataStreamOperatorReport report) throws RegistryException {
    Node  reportNode = getTasksListNode().getChild(tConfig.getTaskId()).getChild("report");
    reportNode.setData(report);
  }
  
  public void create(DataStreamOperatorDescriptor taskConfig, DataStreamOperatorReport report) throws RegistryException {
    Node taskNode = getTasksListNode().getChild(taskConfig.getTaskId());
    taskNode.createChild("report", report, NodeCreateMode.PERSISTENT);
  }
  
  public void suspend(DedicatedTaskContext<DataStreamOperatorDescriptor> context) throws RegistryException {
    suspend(context.getTaskExecutorDescriptor(), context.getTaskId()) ;
  }
  
  public void suspend(TaskExecutorDescriptor executor, DedicatedTaskContext<DataStreamOperatorDescriptor> context) throws RegistryException {
    suspend(executor, context.getTaskId()) ;
  }
  
  public void finish(DedicatedTaskContext<DataStreamOperatorDescriptor> context, TaskStatus taskStatus) throws RegistryException {
    finish(context.getTaskExecutorDescriptor(), context.getTaskId(), taskStatus) ;
  }

  public List<String> getAllExecutorIds() throws RegistryException {
    List<String> executorIds = executorsAllNode.getChildren();
    Collections.sort(executorIds);
    return executorIds;
  }
  
  public List<String> getActiveExecutorIds() throws RegistryException {
    List<String> executorIds = executorsActiveNode.getChildren();
    Collections.sort(executorIds);
    return executorIds;
  }
  
  public List<String> getIdleExecutorIds() throws RegistryException {
    List<String> executorIds = executorsIdleNode.getChildren();
    Collections.sort(executorIds);
    return executorIds;
  }
  
  public List<DataStreamOperatorReportWithStatus> getDataflowTaskRuntimeReportsByExecutorId(String executorId) throws RegistryException {
    List<String> taskIds = executorsAllNode.getChild(executorId).getChild("tasks").getChildren() ;
    return getDataflowTaskRuntimeReports(taskIds) ;
  }
  
  
  public List<DataStreamOperatorReportWithStatus> getDataflowTaskRuntimeReports() throws RegistryException {
    List<String> taskIds = getTasksListNode().getChildren() ;
    return getDataflowTaskRuntimeReports(taskIds);
  }

  List<DataStreamOperatorReportWithStatus> getDataflowTaskRuntimeReports(List<String> taskIds) throws RegistryException {
    String taskListPath = getTasksListNode().getPath();
    List<DataStreamOperatorReportWithStatus> holder = new ArrayList<>();
    for(String selTaskId : taskIds) {
      holder.add(new DataStreamOperatorReportWithStatus(getRegistry(), taskListPath + "/" + selTaskId));
    }
    return holder;
  }
}
