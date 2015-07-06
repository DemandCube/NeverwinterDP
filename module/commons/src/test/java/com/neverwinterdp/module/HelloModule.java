package com.neverwinterdp.module;

import java.util.Map;



@ModuleConfig(name = "HelloModule", autoInstall = false, autostart = false) 
public class HelloModule extends ServiceModule {
  @Override
  protected void configure(Map<String, String> properties) {  
    bind(Hello.class) ;
  }
}