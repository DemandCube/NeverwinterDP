package com.neverwinterdp.scribengin.dataflow.tool.tracking;

import java.util.List;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.vm.client.shell.CommandInput;
import com.neverwinterdp.vm.client.shell.Shell;
import com.neverwinterdp.vm.client.shell.SubCommand;

public class TrackingMonitor extends SubCommand {
  @Parameter(names = "--dataflow-id", required=true, description = "The dataflow id")
  private String dataflowId ;
  
  @Parameter(names = "--report-path", required=true, description = "The report path in the registry")
  private String reportPath ;
  
  @Parameter(names = "--print-period", description = "Pring the report")
  private long printPeriod = 15000;
  
  @Parameter(names = "--max-runtime", description = "The max runtime")
  private long maxRuntime = -1;
  
  @Override
  public void execute(Shell shell, CommandInput cmdInput) throws Exception {
    ScribenginShell scribenginShell = (ScribenginShell) shell;
    TrackingRegistry trackingRegistry = new TrackingRegistry(scribenginShell.getVMClient().getRegistry(), reportPath, false);
    
    if(maxRuntime <= 0) {
      report(scribenginShell, trackingRegistry);
    } else {
      long stopTime = System.currentTimeMillis() + maxRuntime;
      boolean finished = false;
      while(!finished) {
        finished = report(scribenginShell, trackingRegistry);
        if(finished) break;
        if(stopTime < System.currentTimeMillis()) break;
        Thread.sleep(printPeriod);
      }
      //wait to make sure that all the messages are validated and print the report one more time 
      Thread.sleep(5000);
      Thread.sleep(printPeriod);
    }
  }
  
  boolean report(ScribenginShell shell, TrackingRegistry trackingRegistry) throws Exception {
    String[] selId = dataflowId.split(",");
    for(int i = 0; i < selId.length; i++) {
      shell.execute("dataflow info --dataflow-id " + selId[i] + " --show-tasks --show-workers");
    }

    List<TrackingMessageReport> generatedReports = trackingRegistry.getGeneratorReports();
    List<TrackingMessageReport> validatedReports = trackingRegistry.getValidatorReports();
    shell.console().print(TrackingMessageReport.getFormattedReport("Generated Report", generatedReports));
    shell.console().print(TrackingMessageReport.getFormattedReport("Validated Report", validatedReports));

    if(validatedReports.size() == 0) return false;
    boolean validateAllMessges = true;
    for(TrackingMessageReport selReport : validatedReports) {
      if(selReport.getNumOfMessage() == selReport.getProgress()) {
        continue ;
      } else {
        validateAllMessges = false;
        break;
      }
    }
    return validateAllMessges;
  }

  @Override
  public String getDescription() { return "Tracking Monitor command"; }
}
