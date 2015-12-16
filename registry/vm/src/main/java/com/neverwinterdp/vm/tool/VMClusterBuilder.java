package com.neverwinterdp.vm.tool;

import java.util.List;

import com.neverwinterdp.registry.event.WaitingNodeEventListener;
import com.neverwinterdp.registry.event.WaitingRandomNodeEventListener;
import com.neverwinterdp.util.text.TabularFormater;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMStatus;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.command.VMCommand;
import com.neverwinterdp.vm.service.VMService;

public class VMClusterBuilder {
  protected VMClient vmClient ;
  protected String   localAppHome; 
  
  public VMClusterBuilder(String localAppHome, VMClient vmClient) {
    this.localAppHome = localAppHome;
    this.vmClient = vmClient ;
  }
  
  public VMClient getVMClient() { return this.vmClient ; }
  
  protected void init(VMClient vmClient) {
    this.vmClient = vmClient;
  }
  
  public void clean() throws Exception {
  }
  
  public void start() throws Exception {
    if(!vmClient.getRegistry().isConnect()) {
      vmClient.getRegistry().connect() ;
    }
    WaitingNodeEventListener waitingListener = createVMMaster(localAppHome, "vm-master-1");
    waitingListener.waitForEvents(30000);
    TabularFormater info = waitingListener.getTabularFormaterEventLogInfo();
    info.setTitle("Waiting for vm-master events to make sure it is launched properly");
    System.out.println(info.getFormatText()); 
  }
  
  public void shutdown() throws Exception {
    List<VMDescriptor> list = vmClient.getActiveVMDescriptors() ;
    for(VMDescriptor vmDescriptor : list) {
      vmClient.execute(vmDescriptor, new VMCommand.Shutdown());
    }
  }
  
  public WaitingNodeEventListener createVMMaster(String localAppHome, String vmId) throws Exception {
    if(!vmClient.getRegistry().isConnect()) {
      vmClient.getRegistry().connect() ;
    }
    
    WaitingNodeEventListener waitingListener = new WaitingRandomNodeEventListener(vmClient.getRegistry()) ;
    String vmStatusPath = VMService.getVMStatusPath(vmId);
    waitingListener.add(vmStatusPath, VMStatus.RUNNING, "Wait for RUNNING status for vm " + vmId, true);
    String vmHeartbeatPath = VMService.getVMHeartbeatPath(vmId);
    waitingListener.addCreate(vmHeartbeatPath, format("Expect %s has connected heartbeat", vmId), true);
    String vmServiceStatusPath = VMService.MASTER_PATH + "/status";
    waitingListener.add(vmServiceStatusPath, VMService.Status.RUNNING, "Wait for VMService RUNNING status ", true);
    h1(format("Create VM master %s", vmId));
    vmClient.createVMMaster(localAppHome, vmId);
    return waitingListener;
  }
  
  static public void h1(String title) {
    System.out.println("\n\n");
    System.out.println("------------------------------------------------------------------------");
    System.out.println(title);
    System.out.println("------------------------------------------------------------------------");
  }
  
  static public void h2(String title) {
    System.out.println(title);
    StringBuilder b = new StringBuilder() ; 
    for(int i = 0; i < title.length(); i++) {
      b.append("-");
    }
    System.out.println(b) ;
  }
  
  static public String format(String tmpl, Object ... args) {
    return String.format(tmpl, args) ;
  }
}
