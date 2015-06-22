package com.neverwinterdp.scribengin.dataflow;

import java.util.HashMap;
import java.util.Map;

import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.vm.LoggerConfig;

public class DataflowDescriptor {
  private String                         id;
  private String                         name;
  private String                         dataflowAppHome;
  private StorageDescriptor              sourceDescriptor;
  private Map<String, StorageDescriptor> sinkDescriptors;
  private int                            numberOfWorkers               = 1;
  private int                            numberOfExecutorsPerWorker    = 1;
  private long                           maxRunTime                    = -1;
  private long                           maxWaitForWorkerRunningStatus = 45000;
  private long                           maxWaitForAvailableDataStream = 10000;
  private long                           maxWaitForDataRead            = 5000;
  
  private long                           taskMaxExecuteTime            = -1;
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
  public void setNumberOfWorkers(int numberOfWorkers) { this.numberOfWorkers = numberOfWorkers; }
  
  public int getNumberOfExecutorsPerWorker() { return numberOfExecutorsPerWorker; }
  public void setNumberOfExecutorsPerWorker(int number) {
    this.numberOfExecutorsPerWorker = number;
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
  
  public long getMaxWaitForDataRead() { return maxWaitForDataRead; }
  public void setMaxWaitForDataRead(long maxWaitForDataRead) {
    this.maxWaitForDataRead = maxWaitForDataRead;
  }
  
  public long getTaskMaxExecuteTime() { return taskMaxExecuteTime;}
  public void setTaskMaxExecuteTime(long taskMaxExecuteTime) {
    this.taskMaxExecuteTime = taskMaxExecuteTime;
  }
  
  public String getScribe() { return scribe; }
  public void setScribe(String scribe) { this.scribe = scribe; }
  
  public LoggerConfig getLoggerConfig() { return loggerConfig; }
  public void setLoggerConfig(LoggerConfig loggerConfig) { this.loggerConfig = loggerConfig; }
}