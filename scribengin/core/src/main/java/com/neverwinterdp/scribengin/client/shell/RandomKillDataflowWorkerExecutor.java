package com.neverwinterdp.scribengin.client.shell;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.notification.Notifier;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataflowClient;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.test.ExecuteLog;
import com.neverwinterdp.util.text.TabularFormater;
import com.neverwinterdp.vm.VMDescriptor;

public class RandomKillDataflowWorkerExecutor implements Runnable {
  @Parameter(names = "--dataflow-id",  required=true , description = "Specify the id for the dataflow")
  private String dataflowId ;
  
  @Parameter(names = "--wait-before-simulate-failure", description = "Wait before simulate")
  long waitBeforeSimulateFailure = 60000;
  
  
  @Parameter(names = "--failure-period", description = "The command should repeat in this period of time")
  long failurePeriod = 15000;
  
  @Parameter(names = "--max-kill", description = "The command should repeat in this period of time")
  int maxKill = 5;
  
  @Parameter(names = "--simulate-kill", description = "The command should repeat in this period of time")
  boolean simulateKill = false;
  
  private ScribenginShell shell;
  
  public RandomKillDataflowWorkerExecutor() {
  }
  
  public RandomKillDataflowWorkerExecutor(ScribenginShell shell, String dataflowId) {
    init(shell);
  }
  
  public void init(ScribenginShell shell) {
    this.shell = shell;
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
    KillDataflowContext dflKillContext = new KillDataflowContext(scribenginClient, dataflowId) ;
    
    System.err.println("RandomDataflowWorkerKiller: waitBeforeSimulateFailure") ;
    if(waitBeforeSimulateFailure > 0) {
      Thread.sleep(waitBeforeSimulateFailure);
    }
    int simulationCount = 0 ;
    
    while(simulationCount < maxKill) {
      if(!dflKillContext.isKillable()) {
        break;
      }
      dflKillContext.kill();
      Thread.sleep(failurePeriod);
      simulationCount++ ;
    }
    dflKillContext.report(shell);
  }

  public class KillDataflowContext {
    private String dataflowId ;
    private DataflowClient  dflClient ;
    private List<ExecuteLog> executeLogs = new ArrayList<>();
    private boolean terminated = false;
    
    public KillDataflowContext(ScribenginClient scribenginClient, String dataflowId) throws Exception {
      dflClient = scribenginClient.getDataflowClient(dataflowId, 180000);
    }
    
    public boolean isKillable() throws RegistryException {
      if(terminated) return false;
      if(dflClient.getStatus() != DataflowLifecycleStatus.RUNNING) {
        terminated = true;
      }
      return !terminated;
    }
    
    public void kill() throws Exception {
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
}