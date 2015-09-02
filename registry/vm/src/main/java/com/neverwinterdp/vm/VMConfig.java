package com.neverwinterdp.vm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.util.text.StringUtil;

public class VMConfig {
  public enum ClusterEnvironment {
    YARN, YARN_MINICLUSTER, JVM;
    
    public static ClusterEnvironment fromString(String code) {
      for(ClusterEnvironment output : ClusterEnvironment.values()) {
        if(output.toString().equalsIgnoreCase(code)) return output;
      }
      return null;
    }
  }

  @Parameter(names = "--description", description = "Description")
  private String              description;
  
  @Parameter(names = "--name", description = "The registry partition or table")
  private String              name;

  @Parameter(names = "--role", description = "The VM roles")
  private List<String>        roles            = new ArrayList<String>();

  @Parameter(names = "--cpu-cores", description = "The request number of cpu cores")
  private int                 requestCpuCores  = 1;
  @Parameter(names = "--memory", description = "The request amount of memory in MB")
  private int                 requestMemory    = 128;

  @Parameter(names = "--app-home", description = "App Home")
  private String              appHome;

  @Parameter(names = "--app-data-dir", description = "App Data Dir")
  private String              appDataDir = "build/vm";
  
  @Parameter(names = "--log4j-config-url", description = "Log4j Config Url")
  private String              log4jConfigUrl = "classpath:vm-log4j.properties";
  
  @DynamicParameter(names = "--vm-resource:", description = "The resources for the vm")
  private Map<String, String> vmResources      = new LinkedHashMap<String, String>();

  @ParametersDelegate
  private RegistryConfig      registryConfig   = new RegistryConfig();

  @Parameter(
      names = "--self-registration",
      description = "Self create the registation entry in the registry, for master node")
  private boolean             selfRegistration = false;

  @Parameter(names = "--vm-application", description = "The vm application class")
  private String              vmApplication;

  @DynamicParameter(names = "--prop:", description = "The application configuration properties")
  private Map<String, String> properties       = new HashMap<String, String>();

  @DynamicParameter(names = "--hadoop:", description = "The application configuration properties")
  private HadoopProperties    hadoopProperties = new HadoopProperties();

  @Parameter(names = "--environment", description = "Environement YARN, YARN_MINICLUSTER, JVM")
  private ClusterEnvironment         clusterEnvironment      = ClusterEnvironment.JVM;

  public VMConfig() {} 
  
  public VMConfig(String[] args) {
    new JCommander(this, args);
  }
  
  public String getName() { return name; }
  public VMConfig setName(String name) { 
    this.name = name;
    return this;
  }
  
  public List<String> getRoles() { return roles; }
  
  public VMConfig setRoles(List<String> roles) { 
    this.roles = roles; 
    return this;
  }
  
  public VMConfig addRoles(String ... role) {
    if(roles == null) roles = StringUtil.toList(role);
    else StringUtil.addList(roles, role);
    return this;
  }
  
  public int getRequestCpuCores() { return requestCpuCores; }
  public VMConfig setRequestCpuCores(int requestCpuCores) { 
    this.requestCpuCores = requestCpuCores; 
    return this;
  }
  
  public int getRequestMemory() { return requestMemory; }
  public VMConfig setRequestMemory(int requestMemory) { 
    this.requestMemory = requestMemory; 
    return this;
  }
  
  public String getAppHome() { return appHome; }
  public void setAppHome(String appHome) { this.appHome = appHome; }
  
  public String getAppDataDir() { return appDataDir; }
  public void setAppDataDir(String appDataDir) { this.appHome = appDataDir; }
  
  public String getLog4jConfigUrl() { return log4jConfigUrl; }
  public void   setLog4jConfigUrl(String url) { this.log4jConfigUrl = url; }
  
  public Map<String, String> getVmResources() { return vmResources; }
  public void setVmResources(Map<String, String> vmResources) { this.vmResources = vmResources; }
  public void addVMResource(String name, String resource) {
    vmResources.put(name, resource);
  }
  
  
  public RegistryConfig getRegistryConfig() { return registryConfig;}
  public VMConfig setRegistryConfig(RegistryConfig registryConfig) { 
    this.registryConfig = registryConfig;
    return this;
  }
  
  public boolean isSelfRegistration() { return selfRegistration; }
  public VMConfig setSelfRegistration(boolean selfRegistration) { 
    this.selfRegistration = selfRegistration;
    return this;
  }
  
  public String getVmApplication() { return vmApplication;}
  public VMConfig setVmApplication(String vmApplication) { 
    this.vmApplication = vmApplication;
    return this;
  }
  
  public Map<String, String> getProperties() { return properties;}
  public VMConfig setProperties(Map<String, String> appProperties) { 
    this.properties = appProperties;
    return this;
  }
  
  public VMConfig addProperty(String name, String value) {
    properties.put(name, value);
    return this;
  }
  
  public String getProperty(String name) { return properties.get(name); }
  
  public String getProperty(String name, String defaultValue) { 
    String value = properties.get(name); 
    if(value == null) return defaultValue;
    return value ;
  }
  
  public int getPropertyAsInt(String name, int defaultVal) { 
    String val = properties.get(name); 
    return val == null ? defaultVal : Integer.parseInt(val);
  }
  
  public VMConfig addProperty(String name, int value) {
    return addProperty(name, Integer.toString(value));
  }
  
  public long getPropertyAsLong(String name, long defaultVal) { 
    String val = properties.get(name); 
    return val == null ? defaultVal : Long.parseLong(val);
  }
  
  public VMConfig addProperty(String name, long value) {
    return addProperty(name, Long.toString(value));
  }
  
  public boolean getPropertyAsBoolean(String name, boolean defaultVal) { 
    String val = properties.get(name); 
    return val == null ? defaultVal : Boolean.parseBoolean(val);
  }
  
  public VMConfig addProperty(String name, boolean value) {
    return addProperty(name, Boolean.toString(value));
  }
  
  public HadoopProperties getHadoopProperties() { return hadoopProperties; }
  public VMConfig setHadoopProperties(HadoopProperties hadoopProperties) {
    this.hadoopProperties = hadoopProperties;
    return this;
  }
  
  public VMConfig addHadoopProperty(String name, String value) {
    hadoopProperties.put(name, value);
    return this;
  }
  
  public VMConfig addHadoopProperty(Map<String, String> conf) {
    Iterator<Map.Entry<String, String>> i = conf.entrySet().iterator();
    while(i.hasNext()) {
      Map.Entry<String, String> entry = i.next();
      String key = entry.getKey();
      String value = conf.get(key);
      if(value.length() == 0) continue;
      else if(value.indexOf(' ') > 0) continue;
      hadoopProperties.put(key, value);
    }
    return this;
  }
  
  
  public void overrideHadoopConfiguration(Configuration aconf) {
    hadoopProperties.overrideConfiguration(aconf);
  }
  
  public String getDescription() { return description; }
  public VMConfig setDescription(String description) { 
    this.description = description; 
    return this;
  }
  
  public ClusterEnvironment getClusterEnvironment() { return this.clusterEnvironment; }
  public VMConfig setClusterEnvironment(ClusterEnvironment env) { 
    this.clusterEnvironment = env; 
    return this;
  }
  
  public String buildCommand() {
    StringBuilder b = new StringBuilder() ;
    b.append("java ").append(" -Xmx" + requestMemory + "m ").append(VM.class.getName()) ;
    addParameters(b);
    System.out.println("Command: " + b.toString());
    return b.toString() ;
  }
  
  private void addParameters(StringBuilder b) {
    if(name != null) {
      b.append(" --name ").append(name) ;
    }
    
    if(roles != null && roles.size() > 0) {
      b.append(" --role ");
      for(String role : roles) {
        b.append(role).append(" ");
      }
    }
    
    b.append(" --cpu-cores ").append(requestCpuCores) ;
    
    b.append(" --memory ").append(requestMemory) ;
    
    if(appHome != null) {
      b.append(" --app-home ").append(appHome) ;
    }
    
    for(Map.Entry<String, String> entry : vmResources.entrySet()) {
      b.append(" --vm-resource:").append(entry.getKey()).append("=").append(entry.getValue()) ;
    }
    
    b.append(" --vm-application ").append(vmApplication);
    
    if(selfRegistration) {
      b.append(" --self-registration ") ;
    }
    
    b.append(" --registry-connect ").append(registryConfig.getConnect()) ;
    b.append(" --registry-db-domain ").append(registryConfig.getDbDomain()) ;
    b.append(" --registry-implementation ").append(registryConfig.getRegistryImplementation()) ;
    
    b.append(" --environment ").append(clusterEnvironment) ;
    
    b.append(" --log4j-config-url ").append(log4jConfigUrl) ;
    
    for(Map.Entry<String, String> entry : properties.entrySet()) {
      b.append(" --prop:").append(entry.getKey()).append("=").append(entry.getValue()) ;
    }
    
    for(Map.Entry<String, String> entry : hadoopProperties.entrySet()) {
      b.append(" --hadoop:").append(entry.getKey()).append("=").append(entry.getValue()) ;
    }
  }
  
  static public void overrideHadoopConfiguration(Map<String, String> props, Configuration aconf) {
    HadoopProperties hprops = new HadoopProperties() ;
    hprops.putAll(props);
    hprops.overrideConfiguration(aconf);
  }
}
