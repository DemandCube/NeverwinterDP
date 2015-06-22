package com.neverwinterdp.scribengin.dataflow.util;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.util.RegistryDebugger;
import com.neverwinterdp.scribengin.dataflow.activity.util.ActiveActivityNodeDebugger;

public class DataflowRegistryDebugger extends RegistryDebugger {
  private String dataflowPath;
  
  public DataflowRegistryDebugger(Appendable out, Registry registry, String dataflowPath) {
    super(out, registry);
    this.dataflowPath = dataflowPath;
  }
  
  public void enableDataflowTaskDebugger(boolean detailedDebugger) throws RegistryException {
    String taskAssignedPath = dataflowPath + "/tasks/executions/assigned/task-ids";
    watchChild(taskAssignedPath, ".*", new DataflowTaskNodeDebugger(detailedDebugger));
  }
 
  public void enableDataflowActivityDebugger(boolean detailedDebugger) throws RegistryException {
    String activeActivitiesPath = dataflowPath + "/activities/active" ;
    watchChild(activeActivitiesPath, ".*", new ActiveActivityNodeDebugger(detailedDebugger));
  }
  
  public void enableDataflowVMDebugger(boolean detailedDebugger) throws RegistryException {
    String workerActivePath = dataflowPath + "/workers/active";
    watchChild(workerActivePath,  ".*", new DataflowVMDebugger(detailedDebugger));
  }
  
  public void enableDataflowLifecycleDebugger() throws RegistryException {
    String statusPath = dataflowPath + "/status";
    DataflowLifecycleDebugger debugger = new DataflowLifecycleDebugger(dataflowPath) ;
   // watch(statusPath, debugger, true);
    String taskAssignedPath = dataflowPath + "/tasks/executions/assigned/task-ids";
    watchChild(taskAssignedPath, ".*", debugger);
  }
}
