package com.neverwinterdp.scribengin.shell;

import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.DataflowClient;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.DataflowSubmitter;
import com.neverwinterdp.scribengin.dataflow.config.DataflowConfig;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.util.DataflowFormater;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.io.IOUtil;
import com.neverwinterdp.vm.client.shell.Command;
import com.neverwinterdp.vm.client.shell.CommandInput;
import com.neverwinterdp.vm.client.shell.Console;
import com.neverwinterdp.vm.client.shell.Shell;
import com.neverwinterdp.vm.client.shell.SubCommand;
import com.neverwinterdp.yara.snapshot.MetricRegistrySnapshot;

public class DataflowCommand extends Command {
  public DataflowCommand() {
    add("list",               ListDataflow.class) ;
    add("submit",             Submit.class) ;
    add("info",               Info.class) ;
    add("monitor",            Monitor.class) ;
    add("wait-for-status",    WaitForStatus.class) ;
    add("kill-worker-random", KillWorkerRandom.class) ;
  }
  
  @Override
  public String getDescription() {
    return "commands for interacting with dataflows";
  }
  
  static public class ListDataflow extends SubCommand {
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
    }

    @Override
    public String getDescription() {
      return "List all the dataflows";
    }
  }
  
  static public class Submit extends SubCommand {
    @Parameter(names = "--dfs-app-home",  description = "DFS App Home Path")
    private String dfsAppHome ;

    @Parameter(names = "--dataflow-config",  description = "The dataflow descriptor path in the json format")
    private String dataflowConfig ;
    
    @Parameter(names = "--dataflow-id",  description = "Specify the id for the dataflow")
    private String dataflowId ;
    
    @Parameter(names = "--dataflow-max-runtime",  description = "Dataflow max run time")
    private long dataflowMaxRunTime =  -1;
   
    @Parameter(names = "--dataflow-task-switching-period",  description = "Dataflow task switching period")
    private long dataflowTaskSwitchingPeriod =  -1;

    @Parameter(names = "--dataflow-num-of-worker",  description = "Num of worker")
    private int numOfWorker = -1;

    @Parameter(names = "--dataflow-num-of-executor-per-worker",  description = "Num of executor")
    private int numOfExecutorPerWorker = -1;
    
    @Parameter(names = "--dataflow-worker-enable-gc",  description = "enable worker gc")
    private boolean workerEnableGC = false;
    
    @Parameter(names = "--dataflow-worker-profiler-opts",  description = "Add worker profiler opts")
    private String dataflowWorkerProfileOpts ;
    
    @Parameter(names = "--wait-for-running-timeout", description = "The dataflow path to deploy")
    private long waitForRunningTimeout = 120000;
    
    
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      ScribenginShell scribenginShell = (ScribenginShell) shell;
      ScribenginClient client = scribenginShell.getScribenginClient();
      String dataflowJson = IOUtil.getFileContentAsString(dataflowConfig) ;
      DataflowConfig dflConfig = 
        JSONSerializer.INSTANCE.fromString(dataflowJson, DataflowConfig.class);
      if(dataflowId != null) dflConfig.setId(dataflowId);
      if(dataflowMaxRunTime > 0) dflConfig.setMaxRunTime(dataflowMaxRunTime);
      if(dataflowTaskSwitchingPeriod > 0) {
        dflConfig.getWorker().setTaskSwitchingPeriod(dataflowTaskSwitchingPeriod);
      }
      if(numOfWorker > 0) {
        dflConfig.getWorker().setNumOfInstances(numOfWorker);
      }
      if(numOfExecutorPerWorker > 0) {
        dflConfig.getWorker().setNumOfExecutor(numOfExecutorPerWorker);
      }
      if(workerEnableGC) dflConfig.getWorker().setEnableGCLog(workerEnableGC);
      if(dataflowWorkerProfileOpts != null) {
        dflConfig.getWorker().setProfilerOpts(dataflowWorkerProfileOpts);
      }
        
      scala.Console.println(JSONSerializer.INSTANCE.toString(dflConfig));
      DataflowSubmitter submitter = new DataflowSubmitter(client, dflConfig);
      submitter.submit();
      submitter.waitForRunning(waitForRunningTimeout);
      shell.console().println("Finished waiting for the dataflow running status");
    }

    @Override
    public String getDescription() { return "submit a dataflow"; }
  }
  
  static public class Info extends SubCommand {
    @Parameter(names = "--dataflow-id", required=true, description = "The dataflow id")
    String dataflowId ;
    
    @Parameter(names = "--show-tasks", description = "The history dataflow id")
    boolean tasks = false;
    
    @Parameter(names = "--show-workers", description = "Show the active dataflow worker")
    boolean activeWorkers = false;
   
    @Parameter(names = "--show-history-workers", description = "Show the history dataflow worker")
    boolean historyWorkers = false;
    
    @Parameter(names = "--show-activities", description = "The history dataflow id")
    boolean activities = false;
    
    @Parameter(names = "--show-metric", description = "The metric report")
    boolean metricReport = false;
    
    
    @Parameter(names = "--show-all", description = "The history dataflow id")
    boolean all = false;
    
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      ScribenginShell scribenginShell = (ScribenginShell) shell;
      Console console = shell.console();
      ScribenginClient scribenginClient= scribenginShell.getScribenginClient();
      DataflowClient dflClient = scribenginClient.getDataflowClient(dataflowId);
      DataflowRegistry dRegistry = dflClient.getDataflowRegistry();
      info(dRegistry, console, dataflowId);
    }
    
    public void info(DataflowRegistry dRegistry, Console console, String dflId) throws Exception {
      DataflowFormater dflFormater = new DataflowFormater(dRegistry) ;
      
      console.h1("Dataflow " + dRegistry.getDataflowPath());
      
      console.println(dflFormater.getInfo());
      
      if(all || tasks) {
        console.println(dflFormater.getGroupByOperatorDataflowTaskInfo());
        
        console.println(dflFormater.getGroupByExecutorDataflowTaskInfo());
      }
      
      
      if(all || activeWorkers) console.println(dflFormater.getDataflowActiveWorkerInfo());
      
      if(all || historyWorkers) console.println(dflFormater.getDataflowHistoryWorkerInfo());
      
      if(all || activities) console.println(dflFormater.getActivitiesInfo());
      
      if(all || metricReport) {
        List<MetricRegistrySnapshot> snapshots = dRegistry.getWorkerRegistry().getMetrics();
        console.println(MetricRegistrySnapshot.getFormattedText(snapshots));
      }
    }

    @Override
    public String getDescription() {
      return "Display the information of a dataflow";
    }
  }
  
  static public class Monitor extends Info {
    @Parameter(names = "--stop-on-status", description = "Stop on the dataflow status")
    private String stopOnStatus = "TERMINATED";
    
    @Parameter(names = "--dump-period", description = "Dump the information period")
    private long period = 15000;
    
    @Parameter(names = "--timeout" , description = "Dump the information period")
    private long timeout = 3 * 60 * 60 * 1000;
    
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      ScribenginShell scribenginShell = (ScribenginShell) shell;
      ScribenginClient scribenginClient= scribenginShell.getScribenginClient();
      Console console = shell.console();
      DataflowLifecycleStatus stopOnDflStatus = null; 
      if(stopOnStatus != null) {
        stopOnDflStatus = DataflowLifecycleStatus.valueOf(stopOnStatus);
      }
      DataflowClient dflClient = scribenginClient.getDataflowClient(dataflowId);
      DataflowRegistry dRegistry = dflClient.getDataflowRegistry();
      long stopTime = System.currentTimeMillis() + timeout;
      while(stopTime > System.currentTimeMillis()) {
        info(dRegistry, console, dataflowId);
        if(stopOnDflStatus != null) {
          DataflowLifecycleStatus dflStatus = dRegistry.getStatus();
          if(dflStatus.equalOrGreaterThan(stopOnDflStatus)) break;
        }
        Thread.sleep(period);
      }
    }
    
    @Override
    public String getDescription() {
      return "monitor and display more info about dataflows";
    }
  }
  
  static public class WaitForStatus extends SubCommand {
    @Parameter(names = "--dataflow-id", required=true, description = "The dataflow id")
    String dataflowId ;
    
    @Parameter(names = "--status", description = "Stop on the dataflow status")
    private String stopOnStatus = "TERMINATED";
    
    @Parameter(names = "--timeout" , description = "Dump the information period")
    private long timeout = 3 * 60 * 60 * 1000;
    
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      ScribenginShell scribenginShell = (ScribenginShell) shell;
      ScribenginClient scribenginClient= scribenginShell.getScribenginClient();
      DataflowClient dflClient = scribenginClient.getDataflowClient(dataflowId);
      DataflowRegistry dRegistry = dflClient.getDataflowRegistry();
      
      DataflowLifecycleStatus stopOnDflStatus = DataflowLifecycleStatus.valueOf(stopOnStatus);
      long stopTime = System.currentTimeMillis() + timeout;
      while(stopTime > System.currentTimeMillis()) {
        DataflowLifecycleStatus dflStatus = dRegistry.getStatus();
        if(dflStatus.equalOrGreaterThan(stopOnDflStatus)) break;
        Thread.sleep(1000);
      }
    }
    
    @Override
    public String getDescription() {
      return "wait for the dataflow status";
    }
  }
  
  static public class KillWorkerRandom extends SubCommand {
    @ParametersDelegate
    RandomKillDataflowWorkerExecutor executor = new RandomKillDataflowWorkerExecutor();
    
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      executor.init((ScribenginShell) shell);
      executor.run();
    }
    
    @Override
    public String getDescription() { return "Kill Worker"; }
  }
}