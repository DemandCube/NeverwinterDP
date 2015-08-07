package com.neverwinterdp.registry.activity;

import java.util.LinkedList;
import java.util.List;

import com.neverwinterdp.util.text.TabularFormater;

public class ActivityFormatter{
  private Activity activity;
  private List<ActivityStep> activitySteps;
  boolean verbose;

  public ActivityFormatter(Activity activity, List<ActivityStep> activitySteps, boolean verbose) {
    this.activity = activity;
    this.activitySteps = activitySteps;
    this.verbose = verbose;
  }
  
  public String format() { return format(""); }
  
  public String format(String indent) {
    activity.getLogs();
    TabularFormater stepFormatter = 
        new TabularFormater("Step ID", "Type", "Max Retries", "Try", "Exec Time", "Status", "Description");
    
    //Activity Logs
    TabularFormater activityLogsFormatter = new TabularFormater(activity.getId()+" Activity Logs");
    activityLogsFormatter.setIndent(indent+"  ");
    List<String> activityLogs = activity.getLogs();
    if(activityLogs != null){
      for(String log: activityLogs){
        Object[] cells = {log};
        activityLogsFormatter.addRow(cells);
      }
    } else{
      Object[] cells = {"No logs available"};
      activityLogsFormatter.addRow(cells);
    }
    
    //Steps
    stepFormatter.setIndent(indent);
    stepFormatter.setTitle(
                      "Activity: "+
                      "ID = " + this.activity.getId() + ", "+
                      "Type = " + this.activity.getType() + ", "+
                      "Description = " + this.activity.getDescription());
    
    
    LinkedList<TabularFormater> stepLogsFormatterList = new LinkedList<TabularFormater>();
    
    for(ActivityStep step : activitySteps) {
      Object[] cells = {
        step.getId(), step.getType(), step.getMaxRetries(), step.getTryCount(), 
        step.getExecuteTime(), step.getStatus(), step.getDescription()
      } ;
      stepFormatter.addRow(cells);
      
      //Step logs
      TabularFormater stepLogsFormatter = new TabularFormater(step.getId()+" Step Logs, Status:"+step.getStatus());
      stepLogsFormatter.setIndent(indent+"  ");
      List<String> stepLogs = step.getLogs();
      if(stepLogs != null){
        for(String log: stepLogs){
          Object[] cell = {  log };
          stepLogsFormatter.addRow(cell);
        }
      }
      else{
        Object[] cell = {  "No logs available" };
        stepLogsFormatter.addRow(cell);
      }
      
      stepLogsFormatterList.add(stepLogsFormatter);
    }
    
    if(verbose){
      String result = stepFormatter.getFormatText()+"\n    Activity Logs\n"+
          activityLogsFormatter.getFormatText()+"\n    Activity Step Logs\n";
      for(TabularFormater f: stepLogsFormatterList){
        result+=f.getFormatText()+"\n";
      }
      return result;
    }
    else{
      return stepFormatter.getFormatText();
    }
  }
}