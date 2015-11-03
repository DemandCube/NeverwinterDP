package com.neverwinterdp.scribengin.dataflow.worker;

public enum DataflowWorkerStatus {
  CREATE((byte)0), INIT((byte)1), RUNNING((byte)2), 
  PAUSING((byte)3), PAUSE((byte)4), 
  TERMINATED((byte)5), TERMINATED_WITH_INTERRUPT((byte)6), TERMINATED_WITH_ERROR((byte)7);
  
  private byte level;
  
  private DataflowWorkerStatus(byte level) {
    this.level = level;
  }
  
  public int compare(DataflowWorkerStatus other) {
    return level - other.level;
  }
  
  public boolean greaterThan(DataflowWorkerStatus other) {
    return level > other.level ;
  }
  
  public boolean lessThan(DataflowWorkerStatus other) {
    return level < other.level ;
  }
  
  public boolean equalOrGreaterThan(DataflowWorkerStatus other) {
    return level >= other.level ;
  }
}
