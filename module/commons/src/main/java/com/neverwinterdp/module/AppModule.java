package com.neverwinterdp.module;

import java.util.Map;

import com.google.inject.name.Names;
import com.neverwinterdp.os.RuntimeEnv;
import com.neverwinterdp.util.log.LoggerFactory;
import com.neverwinterdp.yara.MetricRegistry;

public class AppModule extends ServiceModule {
  private String hostname;
  String vmName;
  String appHome ;
  String appDataDir;
  
  public AppModule(String hostname, String vmName, String appHome, String appDataDir, Map<String, String> properties) {
    this.hostname = hostname;
    this.vmName = vmName;
    this.appHome = appHome;
    this.appDataDir = appDataDir ;
    setProperties(properties);
  }
  
  @Override
  protected void configure(Map<String, String> properties) {
    Names.bindProperties(binder(), properties) ;
    
    RuntimeEnv runtimeEnv = new RuntimeEnv(hostname, vmName, appHome) ;
    bind(RuntimeEnv.class).toInstance(runtimeEnv);
    
    MetricRegistry metricRegistry = new MetricRegistry(vmName) ;
    bind(MetricRegistry.class).toInstance(metricRegistry);
    
    LoggerFactory loggerFactory = new LoggerFactory() ;
    bind(LoggerFactory.class).toInstance(loggerFactory);
  }
}