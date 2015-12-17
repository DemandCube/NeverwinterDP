package com.neverwinterdp.scribengin.dataflow;

public class WorkerDescriptor {
  private int     numOfInstances          = 2;
  private int     memory                  = 128;
  private int     cpuCores                = 1;
  private int     numOfExecutor           = 2;
  
  private long    taskSwitchingPeriod     = 5000;
  private long    maxWaitForRunningStatus = 60000;
  
  private String  log4jConfigUrl = "classpath:scribengin/log4j/vm-log4j.properties";
  private boolean enableGCLog             = false;
  private String  profilerOpts ;

  public int getNumOfInstances() { return numOfInstances; }
  public void setNumOfInstances(int numOfInstances) {
    this.numOfInstances = numOfInstances;
  }
  
  public int getMemory() { return memory; }
  public void setMemory(int memory) { this.memory = memory; }
  
  public int getCpuCores() { return cpuCores; }
  public void setCpuCores(int cpuCores) { this.cpuCores = cpuCores; }
  
  public int getNumOfExecutor() { return numOfExecutor; }
  public void setNumOfExecutor(int numOfExecutor) { this.numOfExecutor = numOfExecutor; }
  
  public long getTaskSwitchingPeriod() { return taskSwitchingPeriod;}
  public void setTaskSwitchingPeriod(long taskSwitchingPeriod) {
    this.taskSwitchingPeriod = taskSwitchingPeriod;
  }
  
  public long getMaxWaitForRunningStatus() { return maxWaitForRunningStatus; }
  public void setMaxWaitForRunningStatus(long maxWaitForRunningStatus) {
    this.maxWaitForRunningStatus = maxWaitForRunningStatus;
  }
  
  public String getLog4jConfigUrl() { return log4jConfigUrl; }
  public void setLog4jConfigUrl(String log4jConfigUrl) {
    this.log4jConfigUrl = log4jConfigUrl;
  }
  
  public boolean isEnableGCLog() { return enableGCLog; }
  public void setEnableGCLog(boolean enableGCLog) { this.enableGCLog = enableGCLog;}
  
  public String getProfilerOpts() { return profilerOpts;}
  public void setProfilerOpts(String profilerOpts) { this.profilerOpts = profilerOpts; }
}
