package com.neverwinterdp.scribengin.shell;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.DataflowSubmitter;
import com.neverwinterdp.scribengin.dataflow.config.DataflowConfig;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.io.IOUtil;
import com.neverwinterdp.vm.client.shell.Command;
import com.neverwinterdp.vm.client.shell.CommandInput;
import com.neverwinterdp.vm.client.shell.Shell;
import com.neverwinterdp.vm.client.shell.SubCommand;

public class DataflowCommand extends Command {
  public DataflowCommand() {
    add("list", List.class) ;
    add("submit", Submit.class) ;
    add("info", Info.class) ;
  }
  
  @Override
  public String getDescription() {
        return "commands for interacting with dataflows";
  }
  
  static public class List extends SubCommand {
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
    
    @Parameter(names = "--max-run-time", description = "Max Run Time")
    private long maxRunTime = -1l;
    
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
      if(maxRunTime > 0) dflConfig.getWorker().setMaxRunTime(maxRunTime);
      if(dataflowTaskSwitchingPeriod > 0) {
        dflConfig.getWorker().setTaskSwitchingPeriod(dataflowTaskSwitchingPeriod);
      }
      if(numOfWorker > 0) {
        dflConfig.getWorker().setNumOfInstances(numOfWorker);
      }
      if(numOfExecutorPerWorker > 0) {
        dflConfig.getWorker().setNumOfExecutor(numOfExecutorPerWorker);
      }
      
      DataflowSubmitter submitter = new DataflowSubmitter(client, dflConfig);
      submitter.submit();
      submitter.waitForRunning(waitForRunningTimeout);
      shell.console().println("Finished waiting for the dataflow running status");
    }

    @Override
    public String getDescription() { return "submit a dataflow"; }
  }
  
  static public class Info extends SubCommand {
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
    }

    @Override
    public String getDescription() {
      return "Display the information of a dataflow";
    }
  }
}