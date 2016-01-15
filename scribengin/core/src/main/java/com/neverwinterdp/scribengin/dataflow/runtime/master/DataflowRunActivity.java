package com.neverwinterdp.scribengin.dataflow.runtime.master;

import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;

public class DataflowRunActivity extends AllocateWorkerActivity {

  public DataflowRunActivity(MasterService service) {
    super(service);
  }
  
  public void execute() throws Exception {
    super.execute();
    setRunningStatus();
  }
  
  void setRunningStatus() throws Exception {
    DataflowRegistry dflRegistry = service.getDataflowRegistry();
    dflRegistry.setDataflowStatus(DataflowLifecycleStatus.RUNNING);
  }
}
