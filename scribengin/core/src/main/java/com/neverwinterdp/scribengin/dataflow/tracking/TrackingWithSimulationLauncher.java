package com.neverwinterdp.scribengin.dataflow.tracking;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.message.MessageTrackingRegistry;
import com.neverwinterdp.message.MessageTrackingReport;
import com.neverwinterdp.registry.txevent.TXEvent;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.Dataflow;
import com.neverwinterdp.scribengin.dataflow.DataflowClient;
import com.neverwinterdp.scribengin.dataflow.DataflowEvent;
import com.neverwinterdp.scribengin.shell.ScribenginShell;
import com.neverwinterdp.vm.client.VMClient;

public class TrackingWithSimulationLauncher extends TrackingLauncher {
  @Parameter(names = "--simulation-report-period", description = "")
  long simulationReportPeriod = 5000;
  
  @Parameter(names = "--simulation-period", description = "")
  long simulationPeriod = 30000;
  
  @Parameter(names = "--simulation-max", description = "")
  int simulationMax = 2;
  
  private int stopCount  = 0;
  private int startCount = 0;
  
  @Override
  public void execute(ScribenginShell shell, TrackingDataflowBuilder dflBuilder) throws Exception {
    VMClient vmClient = shell.getVMClient();
    submitVMGenerator(vmClient, dflBuilder);
    
    Dataflow<TrackingMessage, TrackingMessage> dfl = dflBuilder.buildDataflow();
    dfl.setDefaultParallelism(8);
    dfl.setDefaultReplication(1);
    startDataflow(shell, dfl);
    
    submitVMValidator(vmClient, dflBuilder);
    String reportPath = dflBuilder.getTrackingConfig().getReportPath();
    for(int i = 0; i < simulationMax; i++) {
      long stopTime = System.currentTimeMillis() + simulationPeriod;
      long reportTime = 0;
      while(System.currentTimeMillis() < stopTime) {
        Thread.sleep(1000);
        reportTime +=  1000;
        if(reportTime >= simulationReportPeriod) {
          shell.execute(
              "plugin com.neverwinterdp.scribengin.dataflow.tracking.TrackingMonitor" +
              "  --dataflow-id " + dfl.getDataflowId() + " --report-path " + reportPath
          );
          reportTime = 0;
        }
      }
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
    stopCount++ ;
    
    MessageTrackingRegistry mtRegistry = dflClient.getDataflowRegistry().getMessageTrackingRegistry();
    while(true) {
      MessageTrackingReport inputReporter  = mtRegistry.getMessageTrackingReporter("input");
      MessageTrackingReport outputReporter = mtRegistry.getMessageTrackingReporter("output");
      long inputCount  = inputReporter.getTrackingCount();
      long outputCount = outputReporter.getTrackingCount();
      System.err.println("Stop: input count = " + inputCount + ", output count = " + outputCount);
      if(inputCount == outputCount ){
        break;
      }
      Thread.sleep(1000);
    }
    shell.execute("dataflow wait-for-status --dataflow-id "  + dataflowId + " --status TERMINATED --timeout 90000 --report-period 10000") ;
  }
}
