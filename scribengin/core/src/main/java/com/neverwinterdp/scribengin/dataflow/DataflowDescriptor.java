package com.neverwinterdp.scribengin.dataflow;

import java.util.HashMap;
import java.util.Map;

public class DataflowDescriptor {
  private String  id;
  private String  name;
  private String  dataflowAppHome;
  private long    maxRunTime         = 90000;
  private int     trackingWindowSize = 1000;
 
  private MasterDescriptor master;
  private WorkerDescriptor worker;
  
  private DataSetDescriptor streamConfig = new DataSetDescriptor();
  private Map<String, OperatorDescriptor> operators = new HashMap<>();
  
  public DataflowDescriptor() {}
  
  public DataflowDescriptor(String name, String id) {
    this.id = id ;
    this.name = name ;
    master = new MasterDescriptor();
    worker = new WorkerDescriptor();
  }
  
  public String getId() { return id; }
  public void setId(String id) { this.id = id; }
  
  public String getName() { return name;}
  public void setName(String name) { this.name = name;}
  
  public String getDataflowAppHome() { return dataflowAppHome; }
  public void setDataflowAppHome(String dataflowAppHome) {
    this.dataflowAppHome = dataflowAppHome;
  }

  public long getMaxRunTime() { return maxRunTime; }
  public void setMaxRunTime(long maxRunTime) { this.maxRunTime = maxRunTime; }
  
  public int getTrackingWindowSize() { return trackingWindowSize; }
  public void setTrackingWindowSize(int size) { this.trackingWindowSize = size; }

  public MasterDescriptor getMaster() { return master; }
  public void setMaster(MasterDescriptor master) { this.master = master; }
  
  public WorkerDescriptor getWorker() { return worker; }
  public void setWorker(WorkerDescriptor worker) { this.worker = worker; }

  public DataSetDescriptor getStreamConfig() { return streamConfig;}
  public void setStreamConfig(DataSetDescriptor streams) { this.streamConfig = streams;}
 
  public Map<String, OperatorDescriptor> getOperators() { return operators; }
  public void setOperators(Map<String, OperatorDescriptor> operators) { this.operators = operators; }
  
  public void addOperator(OperatorDescriptor descriptor) {
    operators.put(descriptor.getName(), descriptor);
  }
  
  public void clearOperators() {
    operators.clear();;
  }
}
