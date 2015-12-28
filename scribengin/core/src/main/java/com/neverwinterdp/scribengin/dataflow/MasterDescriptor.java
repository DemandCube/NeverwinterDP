package com.neverwinterdp.scribengin.dataflow;

public class MasterDescriptor {
  private int     numOfInstances        = 1;
  private int     memory                = 512;
  private int     cpuCores              = 1;
  
  private String  log4jConfigUrl = "classpath:scribengin/log4j/vm-log4j.properties";
  private boolean enableGCLog    = false;
  private String  profilerOpts;

  public int getNumOfInstances() { return numOfInstances; }
  public void setNumOfInstances(int numOfInstances) { this.numOfInstances = numOfInstances;}
  
  public int getMemory() { return memory; }
  public void setMemory(int memory) { this.memory = memory; }

  public int getCpuCores() { return cpuCores; }
  public void setCpuCores(int cpuCores) { this.cpuCores = cpuCores; }
  
  public String getLog4jConfigUrl() { return log4jConfigUrl; }
  public void setLog4jConfigUrl(String log4jConfigUrl) {
    this.log4jConfigUrl = log4jConfigUrl;
  }
  
  public boolean isEnableGCLog() { return enableGCLog; }
  public void setEnableGCLog(boolean enableGCLog) { this.enableGCLog = enableGCLog;}
  
  public String getProfilerOpts() { return profilerOpts; }
  public void setProfilerOpts(String profilerOpts) { this.profilerOpts = profilerOpts; }
}
