package com.neverwinterdp.registry;

import java.lang.reflect.Constructor;

import com.beust.jcommander.Parameter;
import com.google.inject.Singleton;
import com.mycila.jmx.annotation.JmxBean;
import com.mycila.jmx.annotation.JmxField;
import com.neverwinterdp.registry.zk.RegistryImpl;

@Singleton
@JmxBean("role=RegistryConfig, type=RegistryConfig, name=RegistryConfig")
public class RegistryConfig {
  @JmxField
  @Parameter(names = "--registry-connect", description = "The registry connect string")
  private String connect ;
  
  @JmxField
  @Parameter(names = "--registry-db-domain", description = "The registry partition or table")
  private String dbDomain = "/NeverwinterDP";
  
  @JmxField
  @Parameter(names = "--registry-implementation", description = "The registry implementation class")
  private String registryImplementation = RegistryImpl.class.getName();
  
  public String getConnect() { return connect; }
  public void setConnect(String connect) { this.connect = connect; }
  
  public String getDbDomain() { return dbDomain; }
  public void setDbDomain(String dbDomain) { this.dbDomain = dbDomain; }
  
  public String getRegistryImplementation() { return registryImplementation; }
  public void setRegistryImplementation(String registryImplementation) { 
    this.registryImplementation = registryImplementation;
  }

  public Registry newInstance() throws Exception {
    Class<Registry> type = (Class<Registry>) Class.forName(registryImplementation);
    Constructor<Registry> constructor = type.getConstructor(RegistryConfig.class) ;
    return constructor.newInstance(this);
  }

  static public RegistryConfig getDefault() {
    RegistryConfig config = new RegistryConfig();
    config.setConnect("127.0.0.1:2181");
    config.setDbDomain("/NeverwinterDP");
    config.setRegistryImplementation(RegistryImpl.class.getName());
    return config ;
  }
}