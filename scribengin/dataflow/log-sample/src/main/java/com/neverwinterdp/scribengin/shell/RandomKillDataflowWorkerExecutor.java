package com.neverwinterdp.scribengin.shell;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.notification.Notifier;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataflowClient;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.test.ExecuteLog;
import com.neverwinterdp.util.text.TabularFormater;
import com.neverwinterdp.vm.VMDescriptor;

public class RandomKillDataflowWorkerExecutor extends Executor {
  @Parameter(names = "--wait-before-simulate-failure", description = "Wait before simulate")
  public long waitBeforeSimulateFailure = 60000;
  
  
  @Parameter(names = "--failure-period", description = "The command should repeat in this period of time")
  public long failurePeriod = 15000;
  
  @Parameter(names = "--max-kill", description = "The command should repeat in this period of time")
  public int maxKill = 5;
  
  @Parameter(names = "--simulate-kill", description = "The command should repeat in this period of time")
  public boolean simulateKill = false;
  
  String dataflowId ;

  private DataflowClient   dflClient ;
  private List<ExecuteLog> executeLogs = new ArrayList<>();
  
  public RandomKillDataflowWorkerExecutor(ScribenginShell shell, String dataflowId) {
    super(shell) ;
    this.dataflowId = dataflowId;
  }
  
  @Override
  public void run() {
    try {
      execute(shell);
    } catch (Exception e) {
      e.printStackTrace();
      try {
        shell.execute("registry dump");
      } catch (Exception e1) {
      }
    }
  }
  
  void execute(ScribenginShell shell) throws Exception {
    System.err.println("RandomDataflowWorkerKiller: start") ;
    ScribenginClient scribenginClient = shell.getScribenginClient() ;
    dflClient = scribenginClient.getDataflowClient(dataflowId, 180000);

    
    System.err.println("RandomDataflowWorkerKiller: waitBeforeSimulateFailure") ;
    if(waitBeforeSimulateFailure > 0) {
      Thread.sleep(waitBeforeSimulateFailure);
    }
    int simulationCount = 0 ;

    while(simulationCount < maxKill) {
      kill();
      Thread.sleep(failurePeriod);
      simulationCount++ ;
    }
    report(shell);
  }

  public void kill() throws Exception {
    if(dflClient.getStatus() != DataflowLifecycleStatus.RUNNING) {
      return  ;
    }
    Registry registry = dflClient.getRegistry() ;
    String path = "/scribengin/failure-simulation/" + dataflowId + "/workers";
    Notifier notifier = new Notifier(registry, path, "simulation-" + executeLogs.size());
    notifier.initRegistry();
    
    ExecuteLog executeLog = new ExecuteLog() ;
    executeLog.start();
    try {
      VMDescriptor selWorker = selectRandomVM(dflClient.getActiveDataflowWorkers(), notifier);
      if(selWorker != null) {
        executeLog.setDescription("Kill the dataflow worker " + selWorker.getId());
        if(simulateKill) {
          notifier.info("before-simulate-kill", "Before simulate kill " + selWorker.getId());
          dflClient.getScribenginClient().getVMClient().simulateKill(selWorker);
          notifier.info("after-simulate-kill", "After simulate kill " + selWorker.getId());
        } else {
          notifier.info("before-kill", "Before kill " + selWorker.getId());
          dflClient.getScribenginClient().getVMClient().kill(selWorker);
          notifier.info("after-kill", "After kill " + selWorker.getId());
        }
      } else {
        executeLog.setDescription("No available worker is found to kill");
      }
    } finally {
      executeLog.stop();
      notifier.info("kill-result", executeLog.getFormatText());
    }
    executeLogs.add(executeLog);
  }
  
  VMDescriptor selectRandomVM(List<VMDescriptor> vmDescriptors, Notifier notifier) throws Exception {
    if(vmDescriptors.size() == 0) {
      notifier.info("select-random-server", "No server to select");
      return null ;
    }
    Random rand = new Random() ;
    int selIndex = rand.nextInt(vmDescriptors.size()) ;
    VMDescriptor selectedVM =  vmDescriptors.get(selIndex) ;
    StringBuilder vmList = new StringBuilder() ;
    for(VMDescriptor sel : vmDescriptors) {
      if(vmList.length() > 0) vmList.append(", ");
      vmList.append(sel.getId());
    }
    notifier.info("select-random-server", "Select " + selectedVM.getId() + " from " + vmList);
    return selectedVM;
  }
  
  public void report(ScribenginShell shell) throws Exception {
    TabularFormater formater = new TabularFormater("Description", "Success", "Duration") ;
    for(ExecuteLog sel : executeLogs) {
      Object[] cells = {
        sel.getDescription(), sel.isSuccess(), sel.getStop() - sel.getStart()  
      };
      formater.addRow(cells);
    }
    shell.console().println(formater.getFormatText());
  }
}