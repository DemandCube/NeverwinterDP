package com.neverwinterdp.vm.client;

import com.neverwinterdp.util.text.TabularFormater;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.command.CommandResult;
import com.neverwinterdp.vm.service.VMServiceCommand;

public class VMSubmitter {
  private VMClient     vmClient;
  private VMConfig     vmConfig;
  private String       appHome;
  private VMDescriptor vmDescriptor;

  public VMSubmitter(VMClient vmClient, String appHome, VMConfig vmConfig) {
    this.vmClient = vmClient;
    this.appHome  = appHome;
    this.vmConfig = vmConfig ;
  }
  
  public VMDescriptor submit() throws Exception {
    VMDescriptor masterVMDescriptor = vmClient.getMasterVMDescriptor();
    if(appHome != null) {
      VMDescriptor vmMaster = vmClient.getMasterVMDescriptor();
      VMConfig vmConfig = vmMaster.getVmConfig();
      String remoteAppHome = masterVMDescriptor.getVmConfig().getAppHome() + "/apps/" + vmConfig.getName();
      vmConfig.setAppHome(remoteAppHome);
      vmConfig.addVMResource("vm.libs", remoteAppHome + "/libs");
      vmConfig.addVMResource("vm.config", remoteAppHome + "/config");
      vmClient.uploadApp(appHome, remoteAppHome);
    }
    CommandResult<?> result = vmClient.execute(masterVMDescriptor, new VMServiceCommand.Allocate(vmConfig));
    vmDescriptor = result.getResultAs(VMDescriptor.class);
    return vmDescriptor ;
  }
  
  public void waitForRunningStatus(long timeout) throws Exception {
  }
  
  public void waitForTerminated(long timeout) throws Exception {
  }
  
  public String getFormattedResult() {
    TabularFormater formater = new TabularFormater("VM", "") ;
    formater.addRow("VM ID",         vmDescriptor.getId());
    formater.addRow("CPU Cores",     vmDescriptor.getCpuCores());
    formater.addRow("Memory",        vmDescriptor.getMemory());
    formater.addRow("Registry Path", vmDescriptor.getRegistryPath());
    return formater.getFormattedText();
  }
}
