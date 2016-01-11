package com.neverwinterdp.scribengin.dataflow.tracking;

import com.neverwinterdp.registry.txevent.TXEvent;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.Dataflow;
import com.neverwinterdp.scribengin.dataflow.DataflowClient;
import com.neverwinterdp.scribengin.dataflow.DataflowEvent;
import com.neverwinterdp.scribengin.shell.ScribenginShell;
import com.neverwinterdp.vm.client.VMClient;

public class TrackingWithSimulationLauncher extends TrackingLauncher {
  private int stopCount  = 0;
  private int startCount = 0;
  
  
  @Override
  public void execute(ScribenginShell shell, TrackingDataflowBuilder dflBuilder) throws Exception {
    VMClient vmClient = shell.getVMClient();
    submitVMGenerator(vmClient, dflBuilder);
    
    Dataflow<TrackingMessage, TrackingMessage> dfl = dflBuilder.buildDataflow();
    dfl.setDefaultParallelism(5);
    dfl.setDefaultReplication(1);
    startDataflow(shell, dfl);
    
    submitVMValidator(vmClient, dflBuilder);
    
    for(int i = 0; i < 2; i++) {
      Thread.sleep(30000);
      stopDataflow(shell, dflBuilder);
      submitDataflow(shell, dfl);
    }
  }
  
  public void startDataflow(ScribenginShell shell, Dataflow<TrackingMessage, TrackingMessage> dfl) throws Exception {
    System.err.println("--------------------------------------------------------------------------------------");
    System.err.println("Start Dataflow, stopCount = " + startCount);
    System.err.println("--------------------------------------------------------------------------------------");
    submitDataflow(shell, dfl);
    startCount++;
  }
  
  public void stopDataflow(ScribenginShell shell,TrackingDataflowBuilder dflBuilder) throws Exception {
    System.err.println("--------------------------------------------------------------------------------------");
    System.err.println("Stop Dataflow, stopCount = " + stopCount);
    System.err.println("--------------------------------------------------------------------------------------");
    ScribenginShell scribenginShell = (ScribenginShell) shell;
    ScribenginClient scribenginClient = scribenginShell.getScribenginClient();
    String dataflowId = dflBuilder.getDataflowId();
    DataflowClient dflClient = scribenginClient.getDataflowClient(dataflowId);
    TXEvent stopEvent = new TXEvent("stop", DataflowEvent.Stop);
    dflClient.getDataflowRegistry().getMasterRegistry().getMaserEventBroadcaster().broadcast(stopEvent);
    shell.execute("dataflow wait-for-status --dataflow-id "  + dataflowId + " --status TERMINATED --timeout 60000") ;
    shell.execute("dataflow info" + "  --dataflow-id " + dataflowId + " --show-history-workers");
    shell.execute("registry dump");
    stopCount++ ;
  }
}
