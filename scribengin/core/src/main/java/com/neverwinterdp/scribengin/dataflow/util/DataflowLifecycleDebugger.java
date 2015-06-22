package com.neverwinterdp.scribengin.dataflow.util;

import java.util.List;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.util.NodeDebugger;
import com.neverwinterdp.registry.util.RegistryDebugger;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskReport;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskRuntimeReport;
import com.neverwinterdp.util.text.DateUtil;
import com.neverwinterdp.util.text.TabularFormater;

public class DataflowLifecycleDebugger implements NodeDebugger {
  String dataflowPath ;
  
  public DataflowLifecycleDebugger(String dataflowPath) {
    this.dataflowPath = dataflowPath;
  }
  
  @Override
  public void onCreate(RegistryDebugger registryDebugger, Node node) throws Exception {
    registryDebugger.println("DataflowLifecycleDebugger: Node = " + node.getPath() + ", Event = CREATE");
    dump(registryDebugger, node);
  }

  @Override
  public void onModify(RegistryDebugger registryDebugger, Node node) throws Exception {
    registryDebugger.println("DataflowLifecycleDebugger: Node = " + node.getPath() + ", Event = MODIFY");
    dump(registryDebugger, node);
  }

  @Override
  public void onDelete(RegistryDebugger registryDebugger, Node node) throws Exception {
  }
  
  void dump(RegistryDebugger registryDebugger, Node node) throws Exception {
    Registry registry = registryDebugger.getRegistry();
    
    Node dataflowNode = registry.get(dataflowPath);
    DataflowLifecycleStatus  status = dataflowNode.getChild("status").getDataAs(DataflowLifecycleStatus.class);
    
    
    TabularFormater infoFt = new TabularFormater("Dataflow", "");
    infoFt.addRow("Id", dataflowNode.getName());
    infoFt.addRow("Status", status);

    List<DataflowTaskRuntimeReport> reports =  DataflowRegistry.getDataflowTaskReports(registry, dataflowPath);
    String[] header = {
      "Id", "Status", "Acc Commit", "Commit Count", "Last Commit Time", "Start Time", "Finish Time", "Exec Time", "Duration"
    } ;
    TabularFormater taskFt = new TabularFormater(header);
    for(int i = 0; i < reports.size(); i++) {
      DataflowTaskRuntimeReport rtReport = reports.get(i);
      DataflowTaskReport report = rtReport.getReport();
      taskFt.addRow(
          report.getTaskId(), 
          rtReport.getStatus(), 
          report.getAccCommitProcessCount(),
          report.getCommitCount(),
          DateUtil.asCompactDateTime(report.getLastCommitTime()),
          DateUtil.asCompactDateTime(report.getStartTime()),
          DateUtil.asCompactDateTime(report.getFinishTime()),
          report.getAccRuntime() + "ms",
          report.durationTime() + "ms"
      );
    }
    registryDebugger.println(infoFt.getFormattedText());
    registryDebugger.println(taskFt.getFormattedText());
  }
}