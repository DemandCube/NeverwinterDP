package com.neverwinterdp.scribengin.dataflow.config;

import java.util.HashMap;
import java.util.Map;

import com.neverwinterdp.scribengin.storage.StorageDescriptor;

public class DataflowConfig {
  private String  id;
  private String  name;
  private String  dataflowAppHome;
  private int     parallelism ;
  private String  log4jConfigUrl = "classpath:scribengin/log4j/vm-log4j.properties";
 
  private MasterConfig master;
  private WorkerConfig worker;
  
  private Map<String, StorageDescriptor> streams;
  private Map<String, OperatorConfig> operators = new HashMap<>();
  
  public String getId() { return id; }
  public void setId(String id) { this.id = id; }
  
  public String getName() { return name;}
  public void setName(String name) { this.name = name;}
  
  public String getDataflowAppHome() { return dataflowAppHome; }
  public void setDataflowAppHome(String dataflowAppHome) {
    this.dataflowAppHome = dataflowAppHome;
  }

  public int getParallelism() { return parallelism; }
  public void setParallelism(int parallelism) { this.parallelism = parallelism; }
  
  public String getLog4jConfigUrl() { return log4jConfigUrl; }
  public void setLog4jConfigUrl(String log4jConfigUrl) {
    this.log4jConfigUrl = log4jConfigUrl;
  }
  
  public MasterConfig getMaster() { return master; }
  public void setMaster(MasterConfig master) { this.master = master; }
  
  public WorkerConfig getWorker() { return worker; }
  public void setWorker(WorkerConfig worker) { this.worker = worker; }

  public Map<String, StorageDescriptor> getStreams() { return streams;}
  public void setStreams(Map<String, StorageDescriptor> streams) { this.streams = streams;}
 
  public Map<String, OperatorConfig> getOperators() { return operators; }
  public void setOperators(Map<String, OperatorConfig> operators) { this.operators = operators; }
}
