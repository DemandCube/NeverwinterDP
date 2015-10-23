package com.neverwinterdp.scribengin.dataflow.registry;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.task.switchable.SwitchableTaskContext;
import com.neverwinterdp.registry.task.switchable.SwitchableTaskRegistry;
import com.neverwinterdp.scribengin.dataflow.operator.OperatorTaskConfig;
import com.neverwinterdp.scribengin.dataflow.operator.OperatorTaskReport;
import com.neverwinterdp.vm.VMDescriptor;

public class DataflowTaskRegistry extends SwitchableTaskRegistry<OperatorTaskConfig> {
  
  public DataflowTaskRegistry(Registry registry, String path) throws RegistryException {
    init(registry, path, OperatorTaskConfig.class) ;
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
  
  public SwitchableTaskContext<OperatorTaskConfig> take(final VMDescriptor vmDescriptor) throws RegistryException  {
    SwitchableTaskContext<OperatorTaskConfig> tContext = take(vmDescriptor.getRegistryPath());
    return tContext;
  }
  
  public void suspend(String refWorker, SwitchableTaskContext<OperatorTaskConfig> context) throws RegistryException {
    suspend(refWorker, context, false) ;
  }
  
  public void suspend(String refWorker, SwitchableTaskContext<OperatorTaskConfig> context, final boolean disconnectHeartbeat) throws RegistryException {
    suspend(refWorker, context.getTaskTransactionId(), disconnectHeartbeat);
  }

  public void finish(String refWorker, SwitchableTaskContext<OperatorTaskConfig> context) throws RegistryException {
    finish(refWorker, context.getTaskTransactionId());
  }
}
