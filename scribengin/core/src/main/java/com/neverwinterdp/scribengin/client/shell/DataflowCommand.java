package com.neverwinterdp.scribengin.client.shell;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.DataflowClient;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.DataflowSubmitter;
import com.neverwinterdp.scribengin.dataflow.chain.DataflowChainConfig;
import com.neverwinterdp.scribengin.dataflow.chain.OrderDataflowChainSubmitter;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.util.DataflowFormater;
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
    add("info",               Info.class) ;
    add("monitor",            Monitor.class) ;
    add("submit",             Submit.class) ;
    add("submit-chain",       SubmitChain.class) ;
    add("wait-for-status",    WaitForStatus.class) ;
    add("kill-worker-random", KillWorkerRandom.class) ;
  }
  
  @Override
  public String getDescription() {
        return "commands for interacting with dataflows";
  }
  
  static public class Info extends SubCommand {
    @Parameter(names = "--dataflow-id", required=true, description = "The dataflow id")
    String dataflowId ;
    
    @Parameter(names = "--show-tasks", description = "The history dataflow id")
    boolean tasks = false;
    
    @Parameter(names = "--show-workers", description = "The history dataflow id")
    boolean workers = false;
    
    @Parameter(names = "--show-activities", description = "The history dataflow id")
    boolean activities = false;
    
    @Parameter(names = "--show-metric", description = "The metric report")
    boolean metricReport = false;
    
    
    @Parameter(names = "--show-all", description = "The history dataflow id")
    boolean all = false;
    
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      ScribenginShell scribenginShell = (ScribenginShell) shell;
      ScribenginClient scribenginClient= scribenginShell.getScribenginClient();
      Console console = shell.console();
      String[] dflId = this.dataflowId.split(",");
      for(int i = 0; i < dflId.length; i++) {
        DataflowRegistry dRegistry = scribenginClient.getDataflowRegistry(dflId[i]);
        info(dRegistry, console, dflId[i]);
      }
    }
    
    public void info(DataflowRegistry dRegistry, Console console, String dflId) throws Exception {
      DataflowFormater dflFormater = new DataflowFormater(dRegistry) ;
      
      console.h1("Dataflow " + dRegistry.getDataflowPath());
      
      console.println(dflFormater.getInfo());
      
      if(all || tasks) console.println(dflFormater.getDataflowTaskInfo());
      
      if(all || workers) console.println(dflFormater.getDataflowWorkerInfo());
      
      if(all || activities) console.println(dflFormater.getActivitiesInfo());
      
      if(all || metricReport) {
        List<MetricRegistrySnapshot> snapshots = dRegistry.getMetrics();
        console.println(MetricRegistrySnapshot.getFormattedText(snapshots));
      }
    }
    
    @Override
    public String getDescription() {
      return "display more info about dataflows";
    }
  }
  
  static public class Monitor extends Info {
    @Parameter(names = "--stop-on-status",  required=true , description = "Stop on the dataflow status")
    private String stopOnStatus ;
    
    @Parameter(names = "--dump-period", description = "Dump the information period")
    private long period = 15000;
    
    @Parameter(names = "--timeout",  required=true , description = "Dump the information period")
    private long timeout = 3 * 60 * 60 * 1000;
    
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      ScribenginShell scribenginShell = (ScribenginShell) shell;
      ScribenginClient scribenginClient= scribenginShell.getScribenginClient();
      Console console = shell.console();
      List<String> dataflowIdHolder = 
        new ArrayList<String>(Arrays.asList(dataflowId.split(",")));
      DataflowLifecycleStatus stopOnDflStatus = null; 
      if(stopOnStatus != null) {
        stopOnDflStatus = DataflowLifecycleStatus.valueOf(stopOnStatus);
      }
      long stopTime = System.currentTimeMillis() + timeout;
      while(dataflowIdHolder.size() > 0) {
        Iterator<String> i = dataflowIdHolder.iterator();
        while(i.hasNext()) {
          String selDflId = i.next();
          DataflowRegistry dRegistry = scribenginClient.getDataflowRegistry(selDflId);
          info(dRegistry, console, selDflId);
          if(stopOnDflStatus != null) {
            DataflowLifecycleStatus dflStatus = dRegistry.getStatus();
            if(dflStatus.equalOrGreaterThan(stopOnDflStatus)) {
              i.remove();  //monitor dataflow done
            }
          }
          if(System.currentTimeMillis() > stopTime) {
            i.remove();  //monitor dataflow done
          }
        }
        Thread.sleep(period);
      }
    }
    
    @Override
    public String getDescription() {
      return "monitor and display more info about dataflows";
    }
  }
  
  static public class Submit extends SubCommand {
    @Parameter(names = "--dataflow-config",  description = "The dataflow descriptor path in the json format")
    private String dataflowConfig ;
    
    @Parameter(names = "--dfs-app-home",  description = "DFS App Home Path")
    private String dfsAppHome ;

    @Parameter(names = "--dataflow-id",  description = "Specify the id for the dataflow")
    private String dataflowId ;
    
    @Parameter(names = "--max-run-time", description = "Max Run Time")
    private long maxRunTime = -1l;
    
    @Parameter(names = "--wait-for-running-timeout", description = "The dataflow path to deploy")
    private long waitForRunningTimeout = 120000;
    
    
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      ScribenginShell scribenginShell = (ScribenginShell) shell;
      ScribenginClient client = scribenginShell.getScribenginClient();
      String dataflowJson = IOUtil.getFileContentAsString(dataflowConfig) ;
      DataflowDescriptor dflDescriptor = 
        JSONSerializer.INSTANCE.fromString(dataflowJson, DataflowDescriptor.class);
      if(dataflowId != null) dflDescriptor.setId(dataflowId);
      if(maxRunTime > 0) dflDescriptor.setMaxRunTime(maxRunTime);
      DataflowSubmitter submitter = new DataflowSubmitter(client, dfsAppHome, dflDescriptor);
      submitter.submit();
      submitter.waitForRunning(waitForRunningTimeout);
      shell.console().println("Finished waiting for the dataflow running status");
    }

    @Override
    public String getDescription() { return "submit a dataflow"; }
  }
  
  static public class SubmitChain extends SubCommand {
    @Parameter(names = "--dfs-app-home",  description = "DFS App Home Path")
    private String dfsAppHome ;
    
    @Parameter(names = "--dataflow-chain-config",  description = "The dataflow descriptor path in the json format")
    private String dataflowChainConfig ;
    
    @Parameter(names = "--dataflow-max-run-time",  description = "Dataflow max run time")
    private long dataflowMaxRunTime =  -1;
   
    @Parameter(names = "--dataflow-task-switching-period",  description = "Dataflow task switching period")
    private long dataflowTaskSwitchingPeriod =  -1;

    @Parameter(names = "--dataflow-num-of-worker",  description = "Num of worker")
    private int numOfWorker = -1;

    @Parameter(names = "--dataflow-num-of-executor-per-worker",  description = "Num of executor")
    private int numOfExecutorPerWorker = -1;

    
    @Parameter(names = "--wait-for-running-timeout", description = "The dataflow path to deploy")
    private long waitForRunningTimeout = 120000;
    
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      ScribenginShell scribenginShell = (ScribenginShell) shell;
      ScribenginClient client = scribenginShell.getScribenginClient();
      shell.console().println("Submit:");
      shell.console().println("  dataflow chain config: " + dataflowChainConfig);
      shell.console().println("  dfs app home:          "  + dfsAppHome);
      String json = IOUtil.getFileContentAsString(dataflowChainConfig) ;
      DataflowChainConfig config = JSONSerializer.INSTANCE.fromString(json, DataflowChainConfig.class);
      for(DataflowDescriptor sel : config.getDescriptors()) {
        if(dataflowMaxRunTime > 0) sel.setMaxRunTime(dataflowMaxRunTime);
        if(dataflowTaskSwitchingPeriod > 0) sel.setTaskSwitchingPeriod(dataflowTaskSwitchingPeriod);
        if(numOfWorker > 0) sel.setNumberOfWorkers(numOfWorker);
        if(numOfExecutorPerWorker > 0) sel.setNumberOfExecutorsPerWorker(numOfExecutorPerWorker);
      }
      OrderDataflowChainSubmitter submitter = new OrderDataflowChainSubmitter(client, dfsAppHome, config);
      submitter.submit(waitForRunningTimeout);
    }
    
    @Override
    public String getDescription() { return "submit a dataflow"; }
  }
  
  static public class WaitForStatus extends SubCommand {
    @Parameter(names = "--dataflow-id",  required=true , description = "Specify the id for the dataflow")
    private String dataflowId ;
    
    @Parameter(names = "--max-wait-time", description = "The dataflow path to deploy")
    private long maxWaitTime = 180000;
    
    @Parameter(names = "--status",  required=true , description = "Dataflow status")
    private String status ;
    
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      ScribenginShell scribenginShell = (ScribenginShell) shell;
      ScribenginClient scribenginClient = scribenginShell.getScribenginClient();
      DataflowClient dflClient = scribenginClient.getDataflowClient(dataflowId, 30000);
      Console console = shell.console();
      console.h1("Wait For Dataflow " + dataflowId + " Status " + status);
      
      long start = System.currentTimeMillis();
      DataflowLifecycleStatus dflStatus = DataflowLifecycleStatus.valueOf(status);
      dflClient.waitForEqualOrGreaterThanStatus(1000, maxWaitTime, dflStatus);
      long waitTime = System.currentTimeMillis() - start;
      console.println("Wait for the dataflow status " + status + " in " + waitTime + "ms");
    }
    
    @Override
    public String getDescription() { return "wait for the dataflow status"; }
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