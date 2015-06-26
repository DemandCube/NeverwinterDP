package com.neverwinterdp.module;

import java.util.HashMap;
import java.util.Map;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import com.neverwinterdp.os.RuntimeEnv;
import com.neverwinterdp.util.log.LoggerFactory;
import com.neverwinterdp.yara.MetricRegistry;

public class AppModule extends AbstractModule {
  private Map<String, String> properties ;
  
  public AppModule() {
    properties = new HashMap<String, String>() ;
  }
  
  public AppModule(Map<String, String> properties) {
    this.properties = properties ;
  }
  
  @Override
  protected void configure() {
    Names.bindProperties(binder(), properties) ;
    
    RuntimeEnv runtimeEnv = new RuntimeEnv("server", "vm", "build/vm") ;
    bind(RuntimeEnv.class).toInstance(runtimeEnv);
    
    MetricRegistry metricRegistry = new MetricRegistry(properties.get("server.name")) ;
    bind(MetricRegistry.class).toInstance(metricRegistry);
    
    LoggerFactory loggerFactory = new LoggerFactory("[NeverwinterDP]") ;
    bind(LoggerFactory.class).toInstance(loggerFactory);
    
    bind(AppContainer.class).asEagerSingleton();
  }
}
