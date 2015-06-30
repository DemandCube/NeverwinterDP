package com.neverwinterdp.module;

import java.util.Map;

import com.neverwinterdp.vm.service.VMServicePlugin;

@ModuleConfig(name = "VMServiceModule", autoInstall = false, autostart = false) 
public class VMServiceModule extends ServiceModule {
  final static public String NAME = "VMServiceModule" ;
  
  @Override
  protected void configure(Map<String, String> properties) {  
    try {
      bindType(VMServicePlugin.class, properties.get("module.vm.vmservice.plugin"));
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }
}