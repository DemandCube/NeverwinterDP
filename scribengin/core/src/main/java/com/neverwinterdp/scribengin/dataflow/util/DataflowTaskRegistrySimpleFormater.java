package com.neverwinterdp.scribengin.dataflow.util;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.task.TaskStatus;
import com.neverwinterdp.registry.util.NodeFormatter;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskReport;
import com.neverwinterdp.util.ExceptionUtil;
import com.neverwinterdp.util.text.DateUtil;
import com.neverwinterdp.util.text.TabularFormater;

/**
 * @author Tuan
 */
public class DataflowTaskRegistrySimpleFormater extends NodeFormatter {
  private Node taskDescriptorNode ;
  
  public DataflowTaskRegistrySimpleFormater(Node taskNode) {
    this.taskDescriptorNode = taskNode;
  }
  
  @Override
  public String getFormattedText() {
    try {
      if(!taskDescriptorNode.exists()){
        return "Dataflow task activityNode is already deleted or moved to the history";
      }
      
      DataflowTaskDescriptor dflDescriptor = taskDescriptorNode.getDataAs(DataflowTaskDescriptor.class);
      DataflowTaskReport     dflTaskReport = taskDescriptorNode.getChild("report").getDataAs(DataflowTaskReport.class);
      TaskStatus status = taskDescriptorNode.getChild("status").getDataAs(TaskStatus.class);
      TabularFormater formater = new TabularFormater("Name", "Value");
      formater.addRow("ID", dflDescriptor.getTaskId());
      formater.addRow("Status", status);
      
      formater.addRow("Assigned Count", dflTaskReport.getAssignedCount());
      formater.addRow("Process Count", dflTaskReport.getProcessCount());
      formater.addRow("Acc Commit Process Count", dflTaskReport.getAccCommitProcessCount());
      formater.addRow("Commit Count", dflTaskReport.getCommitCount());
      formater.addRow("Last Commit  Time", DateUtil.asCompactDateTime(dflTaskReport.getLastCommitTime()));
      
      formater.addRow("Start  Time", DateUtil.asCompactDateTime(dflTaskReport.getStartTime()));
      formater.addRow("Finish Time", DateUtil.asCompactDateTime(dflTaskReport.getFinishTime()));
      return formater.getFormattedText();
    } catch (Exception e) {
      return ExceptionUtil.getStackTrace(e);
    }
  }
}
