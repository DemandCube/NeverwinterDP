package com.neverwinterdp.nstorage;

import java.text.DecimalFormat;

import com.neverwinterdp.util.text.DateUtil;

public class NStorageReaderDescriptor {
  static DecimalFormat ID_FORMAT = new DecimalFormat("000000");
  
  private String       readerId;
  private long         startedTime;
  private long         finishedTime;
  
  
  public NStorageReaderDescriptor() { }
  
  public NStorageReaderDescriptor(String readerId) {
    this.readerId    = readerId  ;
    this.startedTime = System.currentTimeMillis();
  }
  
  public String getReaderId() { return readerId; }
  public void setReaderId(String id) { this.readerId = id; }
  
  public long getStartedTime() { return startedTime; }
  public void setStartedTime(long startedTime) { this.startedTime = startedTime; }
  
  public long getFinishedTime() { return finishedTime;}
  public void setFinishedTime(long finishedTime) { this.finishedTime = finishedTime; }
  
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append(readerId).append(": {");
    b.append("startedTime=").append(DateUtil.asCompactDateTime(startedTime)).append(", ");
    b.append("finishedTime=").append(DateUtil.asCompactDateTime(finishedTime));
    b.append("}");
    return b.toString();
  }
}
