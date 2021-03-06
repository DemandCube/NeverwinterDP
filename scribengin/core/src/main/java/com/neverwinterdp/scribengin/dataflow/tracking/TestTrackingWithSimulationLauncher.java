package com.neverwinterdp.scribengin.dataflow.tracking;

import java.util.ArrayList;
import java.util.Collections;
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

public class TestTrackingWithSimulationLauncher extends TestTrackingLauncher {
  @Parameter(names = "--simulation-report-period", description = "")
  long simulationReportPeriod = 5000;
  
  @Parameter(names = "--simulation-period", description = "")
  long simulationPeriod = 30000;
  
  @Parameter(names = "--simulation-max", description = "")
  int simulationMax = 1;
  
  boolean simulateKill = false;
  
  List<SimulationLog> simulationLogs = new ArrayList<>();
  
  public TestTrackingWithSimulationLauncher setSimulateKill() {
    simulateKill = true;
    return this;
  }
  
  @Override
  public void execute(ScribenginShell shell, TrackingDataflowBuilder dflBuilder) throws Exception {
    VMClient vmClient = shell.getVMClient();
    submitVMGenerator(vmClient, dflBuilder);
    
    Dataflow dfl = dflBuilder.buildDataflow();
    DataflowSubmitter submitter = submitDataflow(shell, dfl);
    DataflowClient dflClient = submitter.getDataflowClient(60000);
    
    submitVMValidator(vmClient, dflBuilder);
    
    for(int i = 0; i < simulationMax; i++) {
      long stopTime = System.currentTimeMillis() + simulationPeriod;
      long reportTime = 0;
      while(System.currentTimeMillis() < stopTime) {
        Thread.sleep(1000);
        reportTime +=  1000;
        if(reportTime >= simulationReportPeriod) {
          report(shell, dflBuilder);
          reportTime = 0;
        }
      }
      
      int mod = i % 4;
      if(mod == 0) {
        powerFailure(shell, dflBuilder);
      } else if(mod == 1) {
        killWorker(dflClient, dflBuilder.getDataflowId());
        report(shell, dflBuilder);
      } else if(mod == 2) {
        killLeaderMaster(dflClient, dflBuilder.getDataflowId());
        report(shell, dflBuilder);
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
    
    report(shell, dflBuilder);
    
    System.err.println("--------------------------------------------------------------------------------------");
    System.err.println("Resume Dataflow");
    System.err.println("--------------------------------------------------------------------------------------");
    shell.execute("dataflow resume --dataflow-id " + dflBuilder.getDataflowId() + "  --timeout 90000");
    
    //Dataflow<TrackingMessage, TrackingMessage> dfl = dflBuilder.buildDataflow();
    //submitDataflow(shell, dfl);
    log.setFinishedTime(System.currentTimeMillis());
    log.setDescription("Stop then start the dataflow again");
    simulationLogs.add(log);
    report(shell, dflBuilder);
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
    VMDescriptor masterVMDescriptor = dflClient.getDataflowRegistry().getMasterRegistry().getLeaderVMDescriptor();
    if(simulateKill) {
      dflClient.getScribenginClient().getVMClient().simulateKill(masterVMDescriptor);
    } else {
      dflClient.getScribenginClient().getVMClient().kill(masterVMDescriptor, 90000);
    }
    log.setDescription("Kill the leader master " + masterVMDescriptor.getVmId());
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
    VMClient vmClient = dflClient.getScribenginClient().getVMClient();
    
    List<VMDescriptor> allVMs = new ArrayList<>();
    allVMs.addAll(dflClient.getDataflowRegistry().getMasterRegistry().getMasterVMDescriptors());
    allVMs.addAll(dflClient.getActiveDataflowWorkers());
    //randomize the vm list
    Collections.shuffle(allVMs, new Random());
    if(simulateKill) {

      for(VMDescriptor sel : allVMs) {
        vmClient.simulateKill(sel);
      }
    } else {
      for(VMDescriptor sel : allVMs) {
        boolean killed = vmClient.kill(sel, 90000);
        System.err.println("Killed vm " + sel.getVmId() + " = " + killed);
      }
    }
    
    boolean allTerminated = false;
    while(!allTerminated) {
      Thread.sleep(3000);
      allTerminated = true;
      StringBuilder b = new StringBuilder();
      for(VMDescriptor sel : allVMs) {
        VMStatus vmStatus = vmClient.getVMStatus(sel.getVmId()) ;
        b.append(sel.getVmId() + "=" + vmStatus).append(", ");
        if(vmStatus.equalOrLessThan(VMStatus.RUNNING)) {
          allTerminated = false;
        }
      }
      System.err.println("VM status: " + b.toString());
    }
    
    System.err.println("--------------------------------------------------------------------------------------");
    System.err.println("Resume Dataflow");
    System.err.println("--------------------------------------------------------------------------------------");
    shell.execute("dataflow resume --dataflow-id " + dflBuilder.getDataflowId() + "  --timeout 90000");
    //Dataflow<TrackingMessage, TrackingMessage> dfl = dflBuilder.buildDataflow();
    //submitDataflow(shell, dfl);
    
    log.setFinishedTime(System.currentTimeMillis());
    log.setDescription("Kill all the workers and masters, then restart");
    simulationLogs.add(log);
  }

  void report(ScribenginShell shell, TrackingDataflowBuilder dflBuilder) throws Exception {
    shell.execute(
        "plugin com.neverwinterdp.scribengin.dataflow.tracking.TrackingMonitor" +
        "  --dataflow-id " + dflBuilder.getDataflowId()  +  
        " --report-path " + dflBuilder.getTrackingConfig().getTrackingReportPath() //+ " --show-history-vm "
    );
    shell.console().println(SimulationLog.toFormattedText(simulationLogs));
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
      TabularFormater ft = new TabularFormater("#", "Name", "Start", "Finish", "Duration(ms)", "Description");
      ft.setTitle("Simulation Log");
      for(int i = 0; i < logs.size(); i++) {
        SimulationLog sel = logs.get(i) ;
        ft.addRow(
            (i + 1),
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
