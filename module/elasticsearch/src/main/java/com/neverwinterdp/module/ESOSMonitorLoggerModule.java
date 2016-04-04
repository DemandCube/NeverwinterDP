package com.neverwinterdp.module;

import java.util.Map;

import com.neverwinterdp.es.log.MetricLoggerService;
import com.neverwinterdp.es.log.OSMonitorLoggerService;
import com.neverwinterdp.es.sys.SysMetricLoggerService;

@ModuleConfig(name = "ESOSMonitorLoggerModule", autoInstall = false, autostart = false) 
public class ESOSMonitorLoggerModule extends ServiceModule {
  final static public String NAME = "ESOSMonitorLoggerModule";
  
  @Override
  protected void configure(Map<String, String> properties) {  
    bind(OSMonitorLoggerService.class).asEagerSingleton();
    bind(SysMetricLoggerService.class).asEagerSingleton();
    bind(MetricLoggerService.class).asEagerSingleton();
  }
}