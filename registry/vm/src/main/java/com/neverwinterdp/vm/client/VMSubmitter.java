package com.neverwinterdp.vm.client;

import com.neverwinterdp.registry.event.WaitingOrderNodeEventListener;
import com.neverwinterdp.util.text.StringUtil;
import com.neverwinterdp.util.text.TabularFormater;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMStatus;
import com.neverwinterdp.vm.command.CommandResult;
import com.neverwinterdp.vm.service.VMService;
import com.neverwinterdp.vm.service.VMServiceCommand;

public class VMSubmitter {
  private VMClient     vmClient;
  private VMConfig     vmConfig;
  private String       dfsAppHome;
  private String       uploadAppHome ;
  private VMDescriptor vmDescriptor;
  
  public VMSubmitter(VMClient vmClient, String dfsAppHome, VMConfig vmConfig) {
    this.vmClient = vmClient;
    this.dfsAppHome  = dfsAppHome;
    this.vmConfig = vmConfig ;
  }
  
  
  public VMSubmitter setUploadAppHome(String dir) {
    uploadAppHome = dir;
    return this;
  }
  
  
  public VMSubmitter submit() throws Exception {
    if(uploadAppHome != null) {
      if(dfsAppHome == null) {
        String name = uploadAppHome.substring(uploadAppHome.lastIndexOf('/') + 1);
        dfsAppHome = VMClient.APPLICATIONS + "/"  + name;
      }
      vmClient.uploadApp(uploadAppHome, dfsAppHome);
    }
    
    VMDescriptor masterVMDescriptor = vmClient.getMasterVMDescriptor();
    vmConfig.setDfsAppHome(dfsAppHome);
    vmConfig.addVMResource("vm.libs", dfsAppHome + "/libs");
    vmConfig.addVMResource("vm.config", dfsAppHome + "/config");
    vmConfig.setRegistryConfig(vmClient.getRegistry().getRegistryConfig());
    CommandResult<?> result = vmClient.execute(masterVMDescriptor, new VMServiceCommand.Allocate(vmConfig));
    if(result.getErrorStacktrace() != null) {
      System.err.println(result.getErrorStacktrace());
      throw new Exception() ;
    }
    vmDescriptor = result.getResultAs(VMDescriptor.class);
    return this ;
  }
  
  public void waitForStatus(long timeout, VMStatus[] status) throws Exception {
    WaitingOrderNodeEventListener eventListener = new WaitingOrderNodeEventListener(vmClient.getRegistry());
    String vmStatusPath = VMService.getVMStatusPath(vmDescriptor.getVmId());
    String mesg = "Wait for one of the vm status " + StringUtil.join(status, ",");
    eventListener.add(vmStatusPath, status, mesg, true);
    eventListener.waitForEvents(timeout);
  }
  
  public VMSubmitter waitForRunning(long timeout) throws Exception {
    VMStatus[] status = new VMStatus[] {
      VMStatus.RUNNING, VMStatus.TERMINATED
    };
    waitForStatus(timeout, status) ;
    return this;
  }
  
  public void waitForTerminated(long timeout) throws Exception {
    VMStatus[] status = new VMStatus[] { VMStatus.TERMINATED };
    waitForStatus(timeout, status) ;
  }
  
  public String getFormattedResult() {
    TabularFormater formater = new TabularFormater("VM", "") ;
    formater.addRow("VM ID",         vmDescriptor.getVmId());
    formater.addRow("CPU Cores",     vmDescriptor.getCpuCores());
    formater.addRow("Memory",        vmDescriptor.getMemory());
    formater.addRow("Registry Path", vmDescriptor.getRegistryPath());
    return formater.getFormattedText();
  }
}
