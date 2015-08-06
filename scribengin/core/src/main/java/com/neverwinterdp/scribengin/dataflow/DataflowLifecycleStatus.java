package com.neverwinterdp.scribengin.dataflow;

public enum DataflowLifecycleStatus { 
  INIT((byte)0), RUNNING((byte)1), 
  PAUSE((byte)2), STOP((byte)3), 
  FINISH((byte)4), TERMINATED((byte)5) ;
  
  private byte level ;
  
  private DataflowLifecycleStatus(byte level) {
    this.level = level ;
  }
  
  public int compare(DataflowLifecycleStatus other) {
    return level - other.level;
  }
  
  public boolean greaterThan(DataflowLifecycleStatus other) {
    return level > other.level ;
  }
  
  public boolean equalOrGreaterThan(DataflowLifecycleStatus other) {
    return level >= other.level ;
  }
}