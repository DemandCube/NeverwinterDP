package com.neverwinterdp.vm.client.shell;

import java.util.Map;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.util.text.TabularFormater;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMStatus;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.client.VMSubmitter;
import com.neverwinterdp.vm.tool.VMClusterBuilder;
import com.neverwinterdp.vm.util.VMRegistryFormatter;

public class VMCommand extends Command {
  public VMCommand() {
    add("start",              Start.class) ;
    add("shutdown",           Shutdown.class) ;
    add("info",               Info.class) ;
    add("wait-for-vm-status", WaitForVMStatus.class) ;
    add("submit",             Submit.class) ;
    add("upload-app",         UploadApp.class) ;
  }
  
  static public class Start extends SubCommand {
    @Parameter(names = "--upload-app", description = "The path to application home to upload ")
    private String localUploadApp = "./" ;
    
    
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      VMClient vmClient = shell.getVMClient() ;
      VMClusterBuilder clusterBuilder = new VMClusterBuilder(localUploadApp, vmClient) ;
      clusterBuilder.start();
    }

    @Override
    public String getDescription() {
      return "This will start the first vm, vm-master. The vm-master will be responsible for managing, starting, and stopping other VMs upon request";
    }
  }
  
  static public class Shutdown extends SubCommand {
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      VMClient vmClient = shell.getVMClient() ;
      vmClient.shutdown();
    }

    @Override
    public String getDescription() {
      return "This command will shutdown all the running vm, and then shutdown the vm-master. This is a dangerous operation - the cluster will not shutdown properly unless you have already shutdown all running VMs properly";
    }
  }
  
  static public class Info extends SubCommand {
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      VMClient vmClient = shell.getVMClient();
      shell.console().h1("VM Info");
      VMRegistryFormatter regFormatter = new VMRegistryFormatter(null);
      
      TabularFormater tabFormatter = 
          new TabularFormater("Name", "Hostname", "Description", "Memory", "CPU Cores", "Stored Path", "Roles");
      
      
      shell.console().println("Running VM:");
      for(VMDescriptor desc : vmClient.getActiveVMDescriptors()){
        Map<String,String> data = regFormatter.getFormattedMap(desc);
        tabFormatter.addRow(data.get("Name"), data.get("Hostname"),
                            data.get("Description"), data.get("Memory"),
                            data.get("CPU Cores"), data.get("Stored Path"),
                            data.get("Roles"));
      }
      shell.console().println(tabFormatter.getFormatText());
      
      tabFormatter = 
          new TabularFormater("Name", "Hostname", "Description", "Memory", "CPU Cores", "Stored Path", "Roles");
      shell.console().println("History VM:");
      for(VMDescriptor desc : vmClient.getHistoryVMDescriptors()){
        Map<String,String> data = regFormatter.getFormattedMap(desc);
        tabFormatter.addRow(data.get("Name"), data.get("Hostname"),
                            data.get("Description"), data.get("Memory"),
                            data.get("CPU Cores"), data.get("Stored Path"),
                            data.get("Roles"));
      }
      shell.console().println(tabFormatter.getFormatText());
    }

    @Override
    public String getDescription() {
      return "print out info about running and history vms";
    }
  }
  
  static public class Submit extends SubCommand {
    @Parameter(names = "--upload-app", description = "The path to application home to upload ")
    private String uploadAppHome ;
    
    @Parameter(names = "--dfs-app-home", description = "The path to application home to upload ")
    private String dfsAppHome ;
    
    @Parameter(names = "--wait-for-running", description = "The path to application home to upload ")
    private long waitForrunning = 90000;
    
    @Parameter(names = "--wait-for-terminated", description = "The path to application home to upload ")
    private long waitForTerminated = -1;
    
    
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      VMClient vmClient = shell.getVMClient();
      VMSubmitter submitter = new VMSubmitter(vmClient, dfsAppHome, new VMConfig(cmdInput.getRemainArgs()));
      if(uploadAppHome != null) submitter.setUploadAppHome(uploadAppHome);
      submitter.submit();
      submitter.waitForRunning(waitForrunning);
      if(waitForTerminated > 0) {
        submitter.waitForTerminated(waitForTerminated);
      }
      shell.console().print(submitter.getFormattedResult());
    }
    
    @Override
    public String getDescription() { return "Submit a request to run a vm application"; }
  }
  
  static public class UploadApp extends SubCommand {
    @Parameter(names = "--local", description = "The local home path")
    private String localHome ;
    
    @Parameter(names = "--dfs", description = "The dfs home path")
    private String dfsHome ;
    
    
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      VMClient vmClient = shell.getVMClient();
      if(dfsHome == null) {
        String name = localHome.substring(localHome.lastIndexOf('/') + 1);
        dfsHome = VMClient.APPLICATIONS + "/"  + name;
      }
      vmClient.uploadApp(localHome, dfsHome);
    }
    
    @Override
    public String getDescription() { return "Submit a request to run a vm application"; }
  }
  
  static public class WaitForVMStatus extends SubCommand {
    @Parameter(names = "--vm-id", required=true)
    String vmId ;
    
    @Parameter(names = "--vm-status", required=true)
    String vmStatus ;
    
    @Parameter(names = "--max-wait-time", required=true)
    long maxWaitTime = 30000;
    
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      VMClient vmClient = shell.getVMClient();
      shell.console().println("\n");
      shell.console().h1("Wait For VM " + vmId + " With VMSatus " + vmStatus);
      VMStatus status = VMStatus.valueOf(vmStatus);
      long start = System.currentTimeMillis();
      vmClient.waitForEqualOrGreaterThan(vmId, status, 1000, maxWaitTime);
      long waitTime = System.currentTimeMillis() - start;
      shell.console().println("Wait for vm status in " + waitTime + "ms") ;
    }
    
    @Override
    public String getDescription() { return "Wait for vm status"; }
  }

  @Override
  public String getDescription() { return "Commands related to VM instances."; }
}