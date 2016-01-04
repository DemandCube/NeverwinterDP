package com.neverwinterdp.ssm;

public class SegmentsDescriptor {
  private int  lastManagedSegment = 0;
  private long managedFromRecord  = 0;
  private long managedToRecord    = 0;
  
  public int getLastManagedSegment() { return lastManagedSegment; }
  public void setLastManagedSegment(int lastManagedSegment) { this.lastManagedSegment = lastManagedSegment; }
  
  public long getManagedFromRecord() { return managedFromRecord;}
  public void setManagedFromRecord(long managedFromRecord) { this.managedFromRecord = managedFromRecord; }
  
  public long getManagedToRecord() { return managedToRecord;}
  public void setManagedToRecord(long managedToRecord) { this.managedToRecord = managedToRecord; }
  
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append("{");
    b.append("lastManagedSegment=").append(lastManagedSegment).append(", ");
    b.append("managedFromRecord=").append(managedFromRecord).append(", ");
    b.append("managedToRecord=").append(managedToRecord);
    b.append("}");
    return b.toString();
  }
  
}
