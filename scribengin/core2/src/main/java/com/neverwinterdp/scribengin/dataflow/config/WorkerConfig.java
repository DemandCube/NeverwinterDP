package com.neverwinterdp.scribengin.dataflow.config;

public class WorkerConfig {
  private int  numOfInstances          = 2;
  private int  memory                  = 128;
  private int  cpuCores                = 1;
  private int  numOfExecutor           = 2;
  private long taskSwitchingPeriod     = 5000;
  private long maxRunTime              = 90000;
  private long maxWaitForRunningStatus = 60000;

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
  
  public long getMaxRunTime() { return maxRunTime; }
  public void setMaxRunTime(long maxRunTime) { this.maxRunTime = maxRunTime; }
  
  public long getMaxWaitForRunningStatus() { return maxWaitForRunningStatus; }
  public void setMaxWaitForRunningStatus(long maxWaitForRunningStatus) {
    this.maxWaitForRunningStatus = maxWaitForRunningStatus;
  }
}
