package com.neverwinterdp.scribengin.client.shell;

import java.util.List;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.activity.ActivityRegistry;
import com.neverwinterdp.registry.activity.ActivityStep;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.DataflowSubmitter;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskReport;
import com.neverwinterdp.scribengin.dataflow.chain.DataflowChainConfig;
import com.neverwinterdp.scribengin.dataflow.chain.OrderDataflowChainSubmitter;
import com.neverwinterdp.scribengin.dataflow.event.DataflowWaitingEventListener;
import com.neverwinterdp.scribengin.dataflow.worker.DataflowTaskExecutorDescriptor;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.io.IOUtil;
import com.neverwinterdp.vm.client.shell.Command;
import com.neverwinterdp.vm.client.shell.CommandInput;
import com.neverwinterdp.vm.client.shell.Console;
import com.neverwinterdp.vm.client.shell.Shell;
import com.neverwinterdp.vm.client.shell.SubCommand;

public class DataflowCommand extends Command {
  public DataflowCommand() {
    add("info",   Info.class) ;
    add("submit", Submit.class) ;
  }
  
  @Override
  public String getDescription() {
        return "commands for interacting with dataflows";
  }
  
  static public class Info extends SubCommand {
    @Parameter(names = "--dataflow-id", description = "The dataflow id")
    private String dataflowId = "DataflowCommand-unknown";
    
    @Parameter(names = "--show-tasks", description = "The history dataflow id")
    private boolean tasks = false;
    
    @Parameter(names = "--show-workers", description = "The history dataflow id")
    private boolean workers = false;
    
    @Parameter(names = "--show-activities", description = "The history dataflow id")
    private boolean activities = false;
    
    @Parameter(names = "--verbose", description = "Verbose output")
    private boolean verbose = false;
    
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      boolean showAll = false;
      if(!tasks && !workers && !activities){
        showAll = true;
      }
      
      ScribenginShell scribenginShell = (ScribenginShell) shell;
      ScribenginClient scribenginClient= scribenginShell.getScribenginClient();
      DataflowRegistry dRegistry = scribenginClient.getDataflowRegistry(dataflowId);
      
      Console console = shell.console();
      console.h1("Dataflow " + dRegistry.getDataflowPath());
      
      if(tasks || showAll){
        console.println("\nTasks:");
        List<DataflowTaskDescriptor> taskDescriptors = dRegistry.getTaskDescriptors();
        Formater.DataFlowTaskDescriptorList taskList = new Formater.DataFlowTaskDescriptorList(taskDescriptors);
        console.println(taskList.format("All Tasks"));
        List<DataflowTaskReport> taskReports = dRegistry.getTaskReports(taskDescriptors) ;
        Formater.DataflowTaskReportList reportList = new Formater.DataflowTaskReportList(taskReports);
        console.print(reportList.format("Report", "  "));
      }
      
      if(workers || showAll){
        console.println("\nActive Workers:");
        List<String> workers = dRegistry.getActiveWorkerNames();
        for(String worker : workers) {
          List<DataflowTaskExecutorDescriptor> descriptors = dRegistry.getWorkerExecutors(worker);
          console.println("  Worker: " + worker);
          Formater.ExecutorList executorList = new Formater.ExecutorList(descriptors);
          console.println(executorList.format("Executors", "    "));
        }
      }
      
      if(activities || showAll){
        console.println("\nActivities:");
        console.println("  Active Activities:");
        
        ActivityRegistry activityRegistry = dRegistry.getActivityRegistry() ;
        List<Activity> ActiveActivities = activityRegistry.getActiveActivities();
        for(Activity activity : ActiveActivities) {
          List<ActivityStep> steps = activityRegistry.getActivitySteps(activity);
          Formater.ActivityFormatter activityFormatter = new Formater.ActivityFormatter(activity, steps, verbose);
          console.println(activityFormatter.format("    "));
          console.println("");
        }
        
        console.println("  History Activities:");
        List<Activity> historyActivities = activityRegistry.getHistoryActivities();
        for(Activity activity : historyActivities) {
          List<ActivityStep> steps = activityRegistry.getActivitySteps(activity);
          Formater.ActivityFormatter activityFormatter = new Formater.ActivityFormatter(activity, steps, verbose);
          console.println(activityFormatter.format("    "));
        }
      }
    }

    @Override
    public String getDescription() {
      return "display more info about dataflows";
    }
  }
  
  static public class Submit extends SubCommand {
    @Parameter(names = "--dataflow-config",  description = "The dataflow descriptor path in the json format")
    private String dataflowConfig ;
    
    @Parameter(names = "--dataflow-chain-config",  description = "The dataflow descriptor path in the json format")
    private String dataflowChainConfig ;
    
    @Parameter(names = "--deploy", description = "The dataflow path to deploy")
    private String dataflowPath ;
    
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      ScribenginShell scribenginShell = (ScribenginShell) shell;
      if(dataflowConfig != null) {
        submitDataflow(scribenginShell) ;
      } else if(dataflowChainConfig != null) {
        submitDataflowChain(scribenginShell) ;
      } else {
        shell.console().println("Error: Expect the dataflow config or dataflow chain config file");
      }
      
    }

    void submitDataflow(ScribenginShell shell) throws Exception {
      ScribenginClient client = shell.getScribenginClient();
      String dataflowJson = IOUtil.getFileContentAsString(dataflowConfig) ;
      DataflowDescriptor config = JSONSerializer.INSTANCE.fromString(dataflowJson, DataflowDescriptor.class);
      DataflowSubmitter submitter = new DataflowSubmitter(client, dataflowPath, config);
      shell.console().println("Dataflow JSON:");
      shell.console().println(dataflowJson);
      submitter.submit();
      shell.console().println("Submited");
      submitter.waitForRunning(60000);
      shell.console().println("Finished waiting for the dataflow running status");
    } 
    
    void submitDataflowChain(ScribenginShell shell) throws Exception {
      shell.console().println("Submit:");
      shell.console().println("  dataflow chain config: " + dataflowChainConfig);
      shell.console().println("  dataflow local home:"    + dataflowPath);
      String json = IOUtil.getFileContentAsString(dataflowChainConfig) ;
      DataflowChainConfig config = JSONSerializer.INSTANCE.fromString(json, DataflowChainConfig.class);
      OrderDataflowChainSubmitter submitter = 
          new OrderDataflowChainSubmitter(shell.getScribenginClient(), dataflowPath, config);
      submitter.submit(60000);
    }
    
    @Override
    public String getDescription() { return "submit a dataflow"; }
  }
}
