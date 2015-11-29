package com.neverwinterdp.nstorage;

import java.util.ArrayList;
import java.util.List;

public class SegmentReadDescriptor {
  private String       segmentId;
  private long         currentReadRecordIndex;
  private long         currentReadDataPosition;
  private List<String> logs;
  
  public SegmentReadDescriptor() { }
  
  public SegmentReadDescriptor(String segmentId) {
    this.segmentId = segmentId;
  }
  
  public String getSegmentId() { return segmentId; }
  public void setSegmentId(String segmentId) { this.segmentId = segmentId; }
  
  public long getCurrentReadRecordIndex() { return currentReadRecordIndex; }
  public void setCurrentReadRecordIndex(long currentReadRecordIndex) {
    this.currentReadRecordIndex = currentReadRecordIndex;
  }
  
  public long getCurrentReadDataPosition() { return currentReadDataPosition; }
  public void setCurrentReadDataPosition(long currentReadDataPosition) {
    this.currentReadDataPosition = currentReadDataPosition;
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
    b.append("currentReadRecordIndex=").append(currentReadRecordIndex).append(", ");
    b.append("currentReadDataPosition=").append(currentReadDataPosition);
    b.append("}");
    return b.toString();
  }
}