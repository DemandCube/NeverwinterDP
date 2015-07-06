package com.neverwinterdp.module;

import java.util.Map;



@ModuleConfig(name = "VMModule", autoInstall = false, autostart = false) 
public class VMModule extends ServiceModule {
  final static public String NAME = "VMModule" ;
  
  @Override
  protected void configure(Map<String, String> properties) {  
  }
}