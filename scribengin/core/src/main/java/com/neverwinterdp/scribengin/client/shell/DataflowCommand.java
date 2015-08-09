package com.neverwinterdp.scribengin.client.shell;

import com.beust.jcommander.Parameter;
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

public class DataflowCommand extends Command {
  public DataflowCommand() {
    add("info",            Info.class) ;
    add("submit",          Submit.class) ;
    add("submit-chain",    SubmitChain.class) ;
    add("wait-for-status", WaitForStatus.class) ;
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
    
    @Parameter(names = "--show-all", description = "The history dataflow id")
    boolean all = false;
    
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      ScribenginShell scribenginShell = (ScribenginShell) shell;
      ScribenginClient scribenginClient= scribenginShell.getScribenginClient();
      DataflowRegistry dRegistry = scribenginClient.getDataflowRegistry(dataflowId);
      DataflowFormater dflFormater = new DataflowFormater(dRegistry) ;
      
      Console console = shell.console();
      console.h1("Dataflow " + dRegistry.getDataflowPath());
      
      console.println(dflFormater.getInfo());
      
      if(all || tasks) console.println(dflFormater.getDataflowTaskInfo());
      
      if(all || workers) console.println(dflFormater.getDataflowWorkerInfo());
      
      if(all || activities) console.println(dflFormater.getActivitiesInfo());
    }

    @Override
    public String getDescription() {
      return "display more info about dataflows";
    }
  }
  
  static public class Submit extends SubCommand {
    @Parameter(names = "--dataflow-config",  description = "The dataflow descriptor path in the json format")
    private String dataflowConfig ;
    
    @Parameter(names = "--dfs-app-home",  description = "DFS App Home Path")
    private String dfsAppHome ;

    @Parameter(names = "--deploy", description = "The dataflow path to deploy")
    private String dataflowPath ;
    
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
    @Parameter(names = "--dataflow-chain-config",  description = "The dataflow descriptor path in the json format")
    private String dataflowChainConfig ;
    
    @Parameter(names = "--deploy", description = "The dataflow path to deploy")
    private String dataflowPath ;
    
    @Parameter(names = "--wait-for-running-timeout", description = "The dataflow path to deploy")
    private long waitForRunningTimeout = 120000;
    
    
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      ScribenginShell scribenginShell = (ScribenginShell) shell;
      ScribenginClient client = scribenginShell.getScribenginClient();
      shell.console().println("Submit:");
      shell.console().println("  dataflow chain config: " + dataflowChainConfig);
      shell.console().println("  dataflow local home:"    + dataflowPath);
      String json = IOUtil.getFileContentAsString(dataflowChainConfig) ;
      DataflowChainConfig config = JSONSerializer.INSTANCE.fromString(json, DataflowChainConfig.class);
      OrderDataflowChainSubmitter submitter = new OrderDataflowChainSubmitter(client, dataflowPath, config);
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
      console.h1("Wait For Dataflow Status " + status);
      
      long start = System.currentTimeMillis();
      DataflowLifecycleStatus dflStatus = DataflowLifecycleStatus.valueOf(status);
      dflClient.waitForEqualOrGreaterThanStatus(1000, maxWaitTime, dflStatus);
      long waitTime = System.currentTimeMillis() - start;
      console.println("Wait for the dataflow status " + status + " in " + waitTime + "ms");
    }
    
    @Override
    public String getDescription() { return "wait for the dataflow status"; }
  }
}