package com.neverwinterdp.scribengin.dataflow;

import static com.neverwinterdp.vm.tool.VMClusterBuilder.h1;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.SequenceIdTracker;
import com.neverwinterdp.registry.event.WaitingOrderNodeEventListener;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.service.DataflowService;
import com.neverwinterdp.scribengin.dataflow.util.DataflowFormater;
import com.neverwinterdp.scribengin.dataflow.util.DataflowRegistryDebugger;
import com.neverwinterdp.scribengin.service.ScribenginService;
import com.neverwinterdp.scribengin.service.VMScribenginServiceCommand;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.text.StringUtil;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.command.Command;
import com.neverwinterdp.vm.command.CommandResult;

public class DataflowSubmitter {
  private ScribenginClient   scribenginClient;
  private String             dfsDataflowHome;
  private DataflowDescriptor dflDescriptor;

  public DataflowSubmitter(ScribenginClient scribenginClient, String dfsDataflowHome, DataflowDescriptor dflDescriptor) {
    this.scribenginClient  = scribenginClient;
    this.dfsDataflowHome = dfsDataflowHome;
    this.dflDescriptor     = dflDescriptor;
  }

  public void submit() throws Exception {
    Registry registry = scribenginClient.getRegistry();
    VMClient vmClient = scribenginClient.getVMClient() ;
    if(dflDescriptor.getId() == null) {
      SequenceIdTracker dataflowIdTracker = new SequenceIdTracker(registry, ScribenginService.DATAFLOW_ID_TRACKER) ;
      dflDescriptor.setId( dataflowIdTracker.nextSeqId() + "-" + dflDescriptor.getName());
    }

    dflDescriptor.setDataflowAppHome(dfsDataflowHome);
    
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
    try {
      long start = System.currentTimeMillis();
      DataflowLifecycleStatus[] status = new DataflowLifecycleStatus[] {
          DataflowLifecycleStatus.RUNNING, DataflowLifecycleStatus.STOP, DataflowLifecycleStatus.FINISH, DataflowLifecycleStatus.TERMINATED
      };
      waitForStatus(timeout, status) ;
      System.out.println("Wait for RUNNING or TERMINATED status in " + (System.currentTimeMillis() - start) + "ms");
    } catch(Exception ex) {
      dumpDataflowRegistry(System.err);
      throw ex;
    }
  }
  
  public void waitForFinish(long timeout) throws Exception {
    try {
      long start = System.currentTimeMillis();
      DataflowLifecycleStatus[] status = new DataflowLifecycleStatus[] {
          DataflowLifecycleStatus.STOP, DataflowLifecycleStatus.FINISH, DataflowLifecycleStatus.TERMINATED
      };
      waitForStatus(timeout, status) ;
      System.out.println("Wait for TERMINATED status in " + (System.currentTimeMillis() - start) + "ms");
    } catch(Exception ex) {
      dumpDataflowRegistry(System.err);
      throw ex;
    }
  }
  
  public DataflowSubmitter enableDataflowTaskDebugger(Appendable out) throws Exception {
    DataflowRegistryDebugger debugger = scribenginClient.getDataflowRegistryDebugger(out, dflDescriptor);
    debugger.enableDataflowDebugger();
    //debugger.enableDataflowTaskDebugger(false);
    return this ;
  }
  
  public DataflowSubmitter enableAllDebugger(Appendable out) throws Exception {
    DataflowRegistryDebugger debugger = scribenginClient.getDataflowRegistryDebugger(out, dflDescriptor);
    debugger.enableDataflowTaskDebugger(false);
    debugger.enableDataflowVMDebugger(false);
    debugger.enableDataflowActivityDebugger(false);
    return this ;
  }
  
  public void report(Appendable out) throws Exception {
    String dataflowPath = ScribenginService.getDataflowPath(dflDescriptor.getId());
    DataflowFormater dflFt = new DataflowFormater(scribenginClient.getRegistry(), dataflowPath);
    out.append("Dataflow " + dflDescriptor.getId()).append('\n');
    out.append("**************************************************************************************").append('\n');
    out.append(dflFt.getFormattedText()).append('\n');
    out.append("**************************************************************************************").append("\n\n");
  }
  
  public void dumpDataflowRegistry(Appendable out) throws Exception {
    report(out);
    String dataflowStatusPath = ScribenginService.getDataflowPath(dflDescriptor.getId());
    scribenginClient.getRegistry().get(dataflowStatusPath).dump(out);
  }
}
