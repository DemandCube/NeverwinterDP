package com.neverwinterdp.storage.ssm;

import java.util.ArrayList;
import java.util.List;

public class SegmentReadDescriptor {
  
  private String segmentId;
  private long   commitReadRecordIndex;
  private long   commitReadDataPosition;

  private List<String> logs;
  
  public SegmentReadDescriptor() { }
  
  public SegmentReadDescriptor(String segmentId) {
    this.segmentId = segmentId;
  }
  
  public String getSegmentId() { return segmentId; }
  public void setSegmentId(String segmentId) { this.segmentId = segmentId; }
  
  public long getCommitReadRecordIndex() { return commitReadRecordIndex; }
  public void setCommitReadRecordIndex(long idx) {
    this.commitReadRecordIndex = idx;
  }
  
  public long getCommitReadDataPosition() { return commitReadDataPosition; }
  public void setCommitReadDataPosition(long position) {
    this.commitReadDataPosition = position;
  }
  
  public List<String> getLogs() { return logs; }
  public void setLogs(List<String> logs) {  this.logs = logs; }
  public void addLog(String log) {
    if(logs == null) logs = new ArrayList<>();
    logs.add(log);
  }
  
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append(segmentId).append(": {");
    b.append("commitReadRecordIndex=").append(commitReadRecordIndex).append(", ");
    b.append("commitReadDataPosition=").append(commitReadDataPosition);
    b.append("}");
    return b.toString();
  }
}