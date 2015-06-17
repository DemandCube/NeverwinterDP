package com.neverwinterdp.scribengin.dataflow;

import static com.neverwinterdp.vm.tool.VMClusterBuilder.h1;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.SequenceIdTracker;
import com.neverwinterdp.registry.event.WaitingOrderNodeEventListener;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.service.ScribenginService;
import com.neverwinterdp.scribengin.service.VMScribenginServiceCommand;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.text.StringUtil;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.command.Command;
import com.neverwinterdp.vm.command.CommandResult;

public class DataflowSubmitter {
  private ScribenginClient   scribenginClient;
  private String             localDataflowHome;
  private DataflowDescriptor dflDescriptor;

  public DataflowSubmitter(ScribenginClient scribenginClient, String localDataflowHome, DataflowDescriptor dflDescriptor) {
    this.scribenginClient  = scribenginClient;
    this.localDataflowHome = localDataflowHome;
    this.dflDescriptor     = dflDescriptor;
  }

  public void submit() throws Exception {
    Registry registry = scribenginClient.getRegistry();
    VMClient vmClient = scribenginClient.getVMClient() ;
    if(dflDescriptor.getId() == null) {
      SequenceIdTracker dataflowIdTracker = new SequenceIdTracker(registry, ScribenginService.DATAFLOW_ID_TRACKER) ;
      dflDescriptor.setId( dataflowIdTracker.nextSeqId() + "-" + dflDescriptor.getName());
    }
    if(localDataflowHome != null) {
      VMDescriptor vmMaster = vmClient.getMasterVMDescriptor();
      VMConfig vmConfig = vmMaster.getVmConfig();
      String dataflowAppHome = vmConfig.getAppHome() + "/dataflows/" + dflDescriptor.getName();
      dflDescriptor.setDataflowAppHome(dataflowAppHome);
      vmClient.uploadApp(localDataflowHome, dataflowAppHome);
    }
    h1("Submit The Dataflow " + dflDescriptor.getId());
    System.out.println(JSONSerializer.INSTANCE.toString(dflDescriptor)) ;
    VMDescriptor scribenginMaster = scribenginClient.getScribenginMaster();
    Command deployCmd = new VMScribenginServiceCommand.DataflowDeployCommand(dflDescriptor) ;
    CommandResult<Boolean> result = (CommandResult<Boolean>)vmClient.execute(scribenginMaster, deployCmd);
  }
  
  public void waitForStatus(long timeout, DataflowLifecycleStatus[] status) throws Exception {
    WaitingOrderNodeEventListener eventListener = new WaitingOrderNodeEventListener(scribenginClient.getRegistry());
    String dataflowStatusPath = ScribenginService.getDataflowStatusPath(dflDescriptor.getId());
    String mesg = "Wait for one of the dataflow status " + StringUtil.join(status, ",");
    eventListener.add(dataflowStatusPath, status, mesg, true);
    eventListener.waitForEvents(timeout);
  }
  
  public void waitForRunning(long timeout) throws Exception {
    DataflowLifecycleStatus[] status = new DataflowLifecycleStatus[] {
      DataflowLifecycleStatus.RUNNING, DataflowLifecycleStatus.TERMINATED
    };
    waitForStatus(timeout, status) ;
  }
  
  public void waitForTerminated(long timeout) throws Exception {
    DataflowLifecycleStatus[] status = new DataflowLifecycleStatus[] {
      DataflowLifecycleStatus.TERMINATED
    };
    waitForStatus(timeout, status) ;
  }
  
  public DataflowSubmitter enableDataflowTaskDebugger(Appendable out) throws Exception {
    scribenginClient.getDataflowTaskDebugger(out, dflDescriptor, true);
    return this ;
  }
  
  public DataflowSubmitter enableAllDebugger(Appendable out) throws Exception {
    scribenginClient.getDataflowTaskDebugger(System.out, dflDescriptor, false);
    
    scribenginClient.getDataflowVMDebugger(System.out, dflDescriptor, true);
    scribenginClient.getDataflowVMDebugger(System.out, dflDescriptor, false);
    
    scribenginClient.getDataflowActivityDebugger(System.out, dflDescriptor, true);
    scribenginClient.getDataflowActivityDebugger(System.out, dflDescriptor, false);
    return this ;
  }
  
  
  public void dumpDataflowRegistry(Appendable out) throws Exception {
    String dataflowStatusPath = ScribenginService.getDataflowPath(dflDescriptor.getId());
    scribenginClient.getRegistry().get(dataflowStatusPath).dump(out);
  }
}
