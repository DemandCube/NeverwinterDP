package com.neverwinterdp.scribengin.dataflow.tracking;

import com.neverwinterdp.scribengin.dataflow.Dataflow;
import com.neverwinterdp.scribengin.dataflow.DataflowClient;
import com.neverwinterdp.scribengin.dataflow.DataflowSubmitter;
import com.neverwinterdp.scribengin.shell.ScribenginShell;

public class TestSimpleTrackingLauncher extends TestTrackingLauncher {
  @Override
  public void execute(ScribenginShell shell, TrackingDataflowBuilder dflBuilder) throws Exception {
    Dataflow<TrackingMessage, TrackingMessage> dfl = dflBuilder.buildDataflow();
    DataflowSubmitter submitter = submitDataflow(shell, dfl);
    DataflowClient dflClient = submitter.getDataflowClient(60000);
  }
}
