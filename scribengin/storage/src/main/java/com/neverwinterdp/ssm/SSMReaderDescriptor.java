package com.neverwinterdp.ssm;

import java.util.TreeSet;

import com.neverwinterdp.util.text.DateUtil;

public class SSMReaderDescriptor {
  private String          readerId;
  private String          lastReadSegmentId;
  private long            startedTime;
  private long            finishedTime;
  
  public SSMReaderDescriptor() { }
  
  public SSMReaderDescriptor(String readerId) {
    this.readerId    = readerId  ;
    this.startedTime = System.currentTimeMillis();
  }
  
  public String getReaderId() { return readerId; }
  public void setReaderId(String id) { this.readerId = id; }
  
  public String getLastReadSegmentId() { return lastReadSegmentId; }
  public void setLastReadSegmentId(String lastReadSegmentId) {
    this.lastReadSegmentId = lastReadSegmentId;
  }

  public long getStartedTime() { return startedTime; }
  public void setStartedTime(long startedTime) { this.startedTime = startedTime; }
  
  public long getFinishedTime() { return finishedTime;}
  public void setFinishedTime(long finishedTime) { this.finishedTime = finishedTime; }
  
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append(readerId).append(": {");
    b.append("lastReadSegmentId=").append(lastReadSegmentId).append(", ");
    b.append("startedTime=").append(DateUtil.asCompactDateTime(startedTime)).append(", ");
    b.append("finishedTime=").append(DateUtil.asCompactDateTime(finishedTime));
    b.append("}");
    return b.toString();
  }
}
