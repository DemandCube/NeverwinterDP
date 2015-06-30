package com.neverwinterdp.module;

import java.util.Map;

import com.neverwinterdp.es.log.OSMonitorLoggerService;

@ModuleConfig(name = "ESOSMonitorLoggerModule", autoInstall = false, autostart = false) 
public class ESOSMonitorLoggerModule extends ServiceModule {
  final static public String NAME = "ESOSMonitorLoggerModule";
  
  @Override
  protected void configure(Map<String, String> properties) {  
    this.bindType(OSMonitorLoggerService.class, OSMonitorLoggerService.class);
  }
}