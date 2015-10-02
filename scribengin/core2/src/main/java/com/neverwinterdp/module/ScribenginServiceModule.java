package com.neverwinterdp.module;

import java.util.Map;

@ModuleConfig(name = "ScribenginServiceModule", autoInstall = false, autostart = false) 
public class ScribenginServiceModule extends ServiceModule {
  final static public String NAME = "ScribenginServiceModule" ;
  
  @Override
  protected void configure(Map<String, String> props) {  
  }
}