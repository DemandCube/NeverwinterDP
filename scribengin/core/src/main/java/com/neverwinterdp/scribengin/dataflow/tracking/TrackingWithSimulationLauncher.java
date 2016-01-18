package com.neverwinterdp.scribengin.dataflow.tracking;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.message.TrackingWindow;
import com.neverwinterdp.message.TrackingWindowRegistry;
import com.neverwinterdp.message.TrackingWindowReport;
import com.neverwinterdp.registry.txevent.TXEvent;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.Dataflow;
import com.neverwinterdp.scribengin.dataflow.DataflowClient;
import com.neverwinterdp.scribengin.dataflow.DataflowEvent;
import com.neverwinterdp.scribengin.dataflow.DataflowSubmitter;
import com.neverwinterdp.scribengin.shell.ScribenginShell;
import com.neverwinterdp.util.text.DateUtil;
import com.neverwinterdp.util.text.TabularFormater;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMStatus;
import com.neverwinterdp.vm.client.VMClient;

public class TrackingWithSimulationLauncher extends TrackingLauncher {
  @Parameter(names = "--simulation-report-period", description = "")
  long simulationReportPeriod = 5000;
  
  @Parameter(names = "--simulation-period", description = "")
  long simulationPeriod = 30000;
  
  @Parameter(names = "--simulation-max", description = "")
  int simulationMax = 1;
  
  boolean simulateKill = false;
  
  List<SimulationLog> simulationLogs = new ArrayList<>();
  
  public TrackingWithSimulationLauncher setSimulateKill() {
    simulateKill = true;
    return this;
  }
  
  @Override
  public void execute(ScribenginShell shell, TrackingDataflowBuilder dflBuilder) throws Exception {
    VMClient vmClient = shell.getVMClient();
    submitVMGenerator(vmClient, dflBuilder);
    
    Dataflow<TrackingMessage, TrackingMessage> dfl = dflBuilder.buildDataflow();
    DataflowSubmitter submitter = submitDataflow(shell, dfl);
    DataflowClient dflClient = submitter.getDataflowClient(60000);
    
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
              "  --dataflow-id " + dfl.getDataflowId()  +  " --report-path " + reportPath //+ " --show-history-vm "
          );
          reportTime = 0;
          shell.console().println(SimulationLog.toFormattedText(simulationLogs));
        }
      }
      
      int mod = i % 4;
      if(mod == 0) {
        powerFailure(shell, dflBuilder);
      } else if(mod == 1) {
        killWorker(dflClient, dflBuilder.getDataflowId());
      } else if(mod == 2) {
        killLeaderMaster(dflClient, dflBuilder.getDataflowId());
      } else {
        stopStartDataflow(shell, dflBuilder);
      }
    }
  }
  
  
  public void stopStartDataflow(ScribenginShell shell, TrackingDataflowBuilder dflBuilder) throws Exception {
    SimulationLog log = new SimulationLog("stop-start-dataflow");
    System.err.println("--------------------------------------------------------------------------------------");
    System.err.println("Stop Dataflow");
    System.err.println("--------------------------------------------------------------------------------------");
    ScribenginShell scribenginShell = (ScribenginShell) shell;
    ScribenginClient scribenginClient = scribenginShell.getScribenginClient();
    String dataflowId = dflBuilder.getDataflowId();
    DataflowClient dflClient = scribenginClient.getDataflowClient(dataflowId);
    TXEvent stopEvent = new TXEvent("stop", DataflowEvent.Stop);
    dflClient.getDataflowRegistry().getMasterRegistry().getMasterEventBroadcaster().broadcast(stopEvent);
    
    TrackingWindowRegistry mtRegistry = dflClient.getDataflowRegistry().getMessageTrackingRegistry();
    while(true) {
      TrackingWindowReport report  = mtRegistry.getReport();
      
      List<TrackingWindow> commitWindows = mtRegistry.getProgressCommitWindows();
      System.err.println("Stop: tracking count = " + report.getTrackingCount());
      System.err.println(TrackingWindow.toFormattedText("Commit Tracking Windows", commitWindows));
      System.err.println(report.toFormattedText());
      if(commitWindows.size() == 0 && report.getTrackingCount() > 0) break;
      Thread.sleep(1000);
    }
    
    shell.execute(
        "dataflow wait-for-status --dataflow-id "  + dataflowId + 
        " --status STOP --timeout 90000 --report-period 10000") ;
    
    System.err.println("--------------------------------------------------------------------------------------");
    System.err.println("Start Dataflow");
    System.err.println("--------------------------------------------------------------------------------------");
    Dataflow<TrackingMessage, TrackingMessage> dfl = dflBuilder.buildDataflow();
    submitDataflow(shell, dfl);
    log.setFinishedTime(System.currentTimeMillis());
    log.setDescription("Stop then start the dataflow again");
    simulationLogs.add(log);
  }
  
  public void killWorker(DataflowClient dflClient, String dataflowId) throws Exception {
    System.err.println("Kill Worker");
    SimulationLog log = new SimulationLog("kill-dataflow-worker");
    List<VMDescriptor> vmDescriptors = dflClient.getActiveDataflowWorkers();
    if(vmDescriptors.size() == 0) {
      log.setDescription("Do not find an active worker to kill");
    } else {
      Random rand = new Random() ;
      int selIndex = rand.nextInt(vmDescriptors.size()) ;
      VMDescriptor selWorker =  vmDescriptors.get(selIndex) ;
      
      if(selWorker != null) {
        if(simulateKill) {
          log.setDescription("Simulate kill the worker " + selWorker.getVmId());
          dflClient.getScribenginClient().getVMClient().simulateKill(selWorker);
        } else {
          log.setDescription("Kill the worker " + selWorker.getVmId());
          dflClient.getScribenginClient().getVMClient().kill(selWorker, 90000);
        }
      }
    }
    log.setFinishedTime(System.currentTimeMillis());
    simulationLogs.add(log);
  }
  
  public void killLeaderMaster(DataflowClient dflClient, String dataflowId) throws Exception {
    System.err.println("Kill Leader Master");
    SimulationLog log = new SimulationLog("kill-dataflow-master");
    
    if(simulateKill) {
      TXEvent killEvent = new TXEvent("SimulateKillMaster", DataflowEvent.SimulateKillMaster);
      dflClient.getDataflowRegistry().getMasterRegistry().getMasterEventBroadcaster().broadcast(killEvent);
    } else {
      VMDescriptor masterVMDescriptor = dflClient.getDataflowRegistry().getMasterRegistry().getLeaderVMDescriptor();
      log.setDescription("Kill the leader master " + masterVMDescriptor.getVmId());
      dflClient.getScribenginClient().getVMClient().kill(masterVMDescriptor, 90000);
    }
    log.setFinishedTime(System.currentTimeMillis());
    simulationLogs.add(log);
  }
  
  
  public void powerFailure(ScribenginShell shell, TrackingDataflowBuilder dflBuilder) throws Exception {
    SimulationLog log = new SimulationLog("power-failure");
    System.err.println("--------------------------------------------------------------------------------------");
    System.err.println("Kill All the masters and worker");
    System.err.println("--------------------------------------------------------------------------------------");
    String dataflowId = dflBuilder.getDataflowId();
    DataflowClient dflClient = shell.getScribenginClient().getDataflowClient(dataflowId);
    
    List<VMDescriptor> masterVMDescriptors = dflClient.getDataflowRegistry().getMasterRegistry().getMasterVMDescriptors();
    if(simulateKill) {
      TXEvent killMasterEvent = new TXEvent("SimulateKillMaster", DataflowEvent.SimulateKillMaster);
      dflClient.getDataflowRegistry().getMasterRegistry().getMasterEventBroadcaster().broadcast(killMasterEvent);
    } else {
      for(VMDescriptor sel : masterVMDescriptors) {
        boolean killed = dflClient.getScribenginClient().getVMClient().kill(sel, 90000);
        System.err.println("Killed master " + sel.getVmId() + " = " + killed);
      }
    }
    
    VMClient vmClient = dflClient.getScribenginClient().getVMClient();
    
    List<VMDescriptor> activeWorkers = dflClient.getActiveDataflowWorkers();
    for(VMDescriptor sel : activeWorkers) {
      if(simulateKill) {
        vmClient.simulateKill(sel);
      } else {
        boolean killed = vmClient.kill(sel, 90000);
        System.err.println("Killed worker " + sel.getVmId() + " = " + killed);
      }
    }
    
    boolean allTerminated = false;
    while(!allTerminated) {
      Thread.sleep(3000);
      allTerminated = true;
      StringBuilder b = new StringBuilder();
      for(VMDescriptor sel : masterVMDescriptors) {
        VMStatus vmStatus = vmClient.getVMStatus(sel.getVmId()) ;
        b.append(sel.getVmId() + "=" + vmStatus).append(", ");
        if(vmStatus.equalOrLessThan(VMStatus.RUNNING)) {
          allTerminated = false;
        }
      }
      System.err.println("Master status: " + b.toString());
      
      if(!allTerminated) continue;
      b = new StringBuilder();
      for(VMDescriptor sel : activeWorkers) {
        VMStatus vmStatus = dflClient.getScribenginClient().getVMClient().getVMStatus(sel.getVmId()) ;
        b.append(sel.getVmId() + "=" + vmStatus).append(", ");
        if(vmStatus.equalOrLessThan(VMStatus.RUNNING)) {
          allTerminated = false;
        }
      }
      System.err.println("Worker status: " + b.toString());
    }
    
        
    
    System.err.println("--------------------------------------------------------------------------------------");
    System.err.println("Start Dataflow");
    System.err.println("--------------------------------------------------------------------------------------");
    Dataflow<TrackingMessage, TrackingMessage> dfl = dflBuilder.buildDataflow();
    submitDataflow(shell, dfl);
    
    log.setFinishedTime(System.currentTimeMillis());
    log.setDescription("Kill all the workers and masters, then restart");
    simulationLogs.add(log);
  }
  
  static public class SimulationLog {
    String name;
    String description;
    long   startedTime;
    long   finishedTime;
    
    public SimulationLog(String name) {
      this.name = name;
      this.startedTime = System.currentTimeMillis();
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    public long getStartedTime() { return startedTime; }
    public void setStartedTime(long startedTime) { this.startedTime = startedTime; }

    public long getFinishedTime() { return finishedTime; }
    public void setFinishedTime(long finishedTime) { this.finishedTime = finishedTime; }
    
    public long getExecutionTime() { return finishedTime -  startedTime; }
    
    static String toFormattedText(List<SimulationLog> logs) {
      TabularFormater ft = new TabularFormater("Name", "Start", "Finish", "Duration(ms)", "Description");
      ft.setTitle("Simulation Log");
      for(int i = 0; i < logs.size(); i++) {
        SimulationLog sel = logs.get(i) ;
        ft.addRow(
            sel.getName(), 
            DateUtil.asCompactDateTime(sel.startedTime), 
            DateUtil.asCompactDateTime(sel.finishedTime),
            sel.getExecutionTime(),
            sel.getDescription()
        );
      }
      return ft.getFormattedText();
    }
  }
}
