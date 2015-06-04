package com.neverwinterdp.scribengin.dataflow;

import static com.neverwinterdp.vm.tool.VMClusterBuilder.h1;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.SequenceIdTracker;
import com.neverwinterdp.registry.event.WaitingOrderNodeEventListener;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.service.ScribenginService;
import com.neverwinterdp.scribengin.service.VMScribenginServiceCommand;
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

  public void submit(long timeout) throws Exception {
    submit(false, false, false, timeout) ;
  }
  
  public void submitAndWaitForInitStatus(long timeout) throws Exception {
    submit(true, false, false, timeout) ;
  }
  
  public void submitAndWaitForRunningStatus(long timeout) throws Exception {
    submit(true, true, false, timeout) ;
  }
  
  public void submitAndWaitForTerminatedStatus(long timeout) throws Exception {
    submit(true, true, true, timeout) ;
  }

  void submit(boolean waitForInit, boolean waitForRunning, boolean waitForTerminated, long timeout) throws Exception {
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
    h1("Submit the dataflow " + dflDescriptor.getId());
    
    WaitingOrderNodeEventListener eventListener = new WaitingOrderNodeEventListener(registry);
    if(waitForInit) {
      waitDataflowStatus(eventListener, "Expect dataflow init status", dflDescriptor, DataflowLifecycleStatus.INIT);
    }
    if(waitForRunning) {
      waitDataflowStatus(eventListener, "Expect dataflow running status", dflDescriptor, DataflowLifecycleStatus.RUNNING);
    }
    if(waitForTerminated) {
      waitDataflowStatus(eventListener, "Expect dataflow terminated status", dflDescriptor, DataflowLifecycleStatus.TERMINATED);
    }
    VMDescriptor scribenginMaster = scribenginClient.getScribenginMaster();
    Command deployCmd = new VMScribenginServiceCommand.DataflowDeployCommand(dflDescriptor) ;
    CommandResult<Boolean> result = (CommandResult<Boolean>)vmClient.execute(scribenginMaster, deployCmd);
    
    try { 
      eventListener.waitForEvents(timeout);
    } catch(Exception ex) {
      registry.get("/scribengin/dataflow/all/info-log-persister-dataflow-1").dump(System.out);
      throw ex; 
    } finally {
      System.out.println(eventListener.getTabularFormaterEventLogInfo().getFormatText()); 
    }
  }
  
  public void waitDataflowStatus(WaitingOrderNodeEventListener listener, String desc, DataflowDescriptor descriptor, DataflowLifecycleStatus status) throws Exception {
    String dataflowStatusPath = ScribenginService.getDataflowStatusPath(descriptor.getId());
    listener.add(dataflowStatusPath, desc, status);
  }
}
