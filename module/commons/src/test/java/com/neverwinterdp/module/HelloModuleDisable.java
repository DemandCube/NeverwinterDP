package com.neverwinterdp.module;

import java.util.Map;


/**
 * @author Tuan
 * This module class is used to configure the available services for the module. The default parameters and the default 
 * action such auto install when the module is installed, auto start all the services in the module when the module is
 * installed
 */
@ModuleConfig(name = "HelloModuleDisable", autoInstall = false, autostart = false) 
public class HelloModuleDisable extends ServiceModule {
  @Override
  protected void configure(Map<String, String> properties) {  
    bindType("HelloService", Hello.class); 
  }
}