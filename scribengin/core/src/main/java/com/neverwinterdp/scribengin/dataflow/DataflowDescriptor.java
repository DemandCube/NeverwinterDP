package com.neverwinterdp.scribengin.dataflow;

import java.util.HashMap;
import java.util.Map;

import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.vm.LoggerConfig;

public class DataflowDescriptor {
  static public enum DataflowTaskExecutorType { Switchable, Dedicated }
  
  private String                         id;
  private String                         name;
  private String                         dataflowAppHome;
  private StorageDescriptor              sourceDescriptor;
  private Map<String, StorageDescriptor> sinkDescriptors;
  private int                            numberOfWorkers               = 1;
  private int                            numberOfExecutorsPerWorker    = 1;
  private DataflowTaskExecutorType       dataflowTaskExecutorType      = DataflowTaskExecutorType.Switchable;
  private int                            workerMemory                  =  128;
  private long                           maxRunTime                    = -1;
  private long                           maxWaitForWorkerRunningStatus = 30000;
  private long                           maxWaitForAvailableDataStream = 10000;
  
  private long                           taskSwitchingPeriod           = 30000;
  private String                         scribe;
  private LoggerConfig                   loggerConfig                  = new LoggerConfig();

  public String getId() { return id; }
  public void setId(String id)  { this.id = id; }
  
  public String getName() { return name; }
  public void setName(String name) { this.name = name; }
  
  public String getDataflowAppHome() { return dataflowAppHome; }
  public void setDataflowAppHome(String dataflowAppHome) { this.dataflowAppHome = dataflowAppHome;  }

  public StorageDescriptor getSourceDescriptor() { return sourceDescriptor;}
  public void setSourceDescriptor(StorageDescriptor sourceDescriptor) { this.sourceDescriptor = sourceDescriptor;}

  public void addSinkDescriptor(String name, StorageDescriptor descriptor) {
    if(sinkDescriptors == null) sinkDescriptors = new HashMap<String, StorageDescriptor>();
    sinkDescriptors.put(name, descriptor);
  }
  
  public Map<String, StorageDescriptor> getSinkDescriptors() { return sinkDescriptors; }
  public void setSinkDescriptors(Map<String, StorageDescriptor> sinkDescriptors) {
    this.sinkDescriptors = sinkDescriptors;
  }
  
  public int getNumberOfWorkers() { return numberOfWorkers; }
  public void setNumberOfWorkers(int number) { this.numberOfWorkers = number; }
  
  public int getNumberOfExecutorsPerWorker() { return numberOfExecutorsPerWorker; }
  public void setNumberOfExecutorsPerWorker(int number) {
    this.numberOfExecutorsPerWorker = number;
  }
  
  public DataflowTaskExecutorType getDataflowTaskExecutorType() { return dataflowTaskExecutorType; }
  public void setDataflowTaskExecutorType(DataflowTaskExecutorType type) {
    this.dataflowTaskExecutorType = type;
  }
  
  public int getWorkerMemory() { return workerMemory; }
  public void setWorkerMemory(int memory) {
    this.workerMemory = memory;
  }
  
  public long getMaxRunTime() { return maxRunTime; }
  public void setMaxRunTime(long maxRunTime) { this.maxRunTime = maxRunTime;}
  
  public long getMaxWaitForWorkerRunningStatus() { return maxWaitForWorkerRunningStatus; }
  public void setMaxWaitForWorkerRunningStatus(long maxWaitForWorkerRunningStatus) {
    this.maxWaitForWorkerRunningStatus = maxWaitForWorkerRunningStatus;
  }
  
  public long getMaxWaitForAvailableDataStream() { return maxWaitForAvailableDataStream; }
  public void setMaxWaitForAvailableDataStream(long waitForAvailableDataStream) {
    this.maxWaitForAvailableDataStream = waitForAvailableDataStream;
  }
  
  public long getTaskSwitchingPeriod() { return taskSwitchingPeriod;}
  public void setTaskSwitchingPeriod(long period) {
    this.taskSwitchingPeriod = period;
  }
  
  public String getScribe() { return scribe; }
  public void setScribe(String scribe) { this.scribe = scribe; }
  
  public LoggerConfig getLoggerConfig() { return loggerConfig; }
  public void setLoggerConfig(LoggerConfig loggerConfig) { this.loggerConfig = loggerConfig; }
}