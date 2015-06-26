package com.neverwinterdp.module;

import java.util.Map;


@ModuleConfig(name = "HelloModule", autoInstall = true, autostart = true) 
public class HelloModule extends ServiceModule {
  protected void configure(Map<String, String> properties) {  
    properties.put("hello:hello", "hello map property") ;
    
    bind(Hello.class) ;
  }
}