package com.neverwinterdp.vm;

public enum VMStatus {
  ALLOCATED((byte)0), INIT((byte)1), RUNNING((byte)2), TERMINATED((byte)3); 
  
  private byte level ;
  
  private VMStatus(byte level) {
    this.level = level ;
  }
  
  public int compare(VMStatus other) {
    return level - other.level;
  }
  
  public boolean greaterThan(VMStatus other) {
    return level > other.level ;
  }
  
  public boolean equalOrGreaterThan(VMStatus other) {
    return level >= other.level ;
  }
}
