package com.neverwinterdp.module;

import java.util.Map;

import com.neverwinterdp.vm.environment.yarn.SyncYarnManager;
import com.neverwinterdp.vm.environment.yarn.YarnManager;
import com.neverwinterdp.vm.service.VMServicePlugin;

@ModuleConfig(name = "VMServiceModule", autoInstall = false, autostart = false) 
public class VMServiceModule extends ServiceModule {
  final static public String NAME = "VMServiceModule" ;
  
  @Override
  protected void configure(Map<String, String> properties) {  
    try {
      String vmServicePlugin = properties.get("module.vm.vmservice.plugin");
      if(vmServicePlugin.indexOf("Yarn") >= 0) {
        bindType(YarnManager.class, SyncYarnManager.class);
      }
      bindType(VMServicePlugin.class, vmServicePlugin);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }
}