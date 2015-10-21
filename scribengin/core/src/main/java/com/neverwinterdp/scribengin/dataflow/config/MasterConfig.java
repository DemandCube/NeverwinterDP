package com.neverwinterdp.scribengin.dataflow.config;

public class MasterConfig {
  private int numOfInstances = 2;
  private int memory   =  128;
  private int cpuCores = 1;
  
  public int getNumOfInstances() { return numOfInstances; }
  public void setNumOfInstances(int numOfInstances) { this.numOfInstances = numOfInstances;}
  
  public int getMemory() { return memory; }
  public void setMemory(int memory) { this.memory = memory; }

  public int getCpuCores() { return cpuCores; }
  public void setCpuCores(int cpuCores) { this.cpuCores = cpuCores; }
}
