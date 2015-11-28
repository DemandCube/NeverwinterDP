package com.neverwinterdp.nstorage.segment;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.util.text.DateUtil;

public class ReaderDescriptor {
  static DecimalFormat ID_FORMAT = new DecimalFormat("000000");
  
  private String       id;
  private String       reader;
  private long         startedTime;
  private long         finishedTime;
  private String       currentSegment;
  private long         currentReadRecordIndex;
  private long         currentReadDataPosition;
  private List<String> logs;
  
  public ReaderDescriptor() { }
  
  public ReaderDescriptor(int id, String reader) {
    this.id          = reader + "-" + ID_FORMAT.format(id) ;
    this.reader      = reader;
    this.startedTime = System.currentTimeMillis();
  }
  
  public String getId() { return id; }
  public void setId(String id) { this.id = id; }
  
  public String getReader() { return reader; }
  public void setReader(String reader) { this.reader = reader; }
  
  public long getStartedTime() { return startedTime; }
  public void setStartedTime(long startedTime) { this.startedTime = startedTime; }
  
  public long getFinishedTime() { return finishedTime;}
  public void setFinishedTime(long finishedTime) { this.finishedTime = finishedTime; }
  
  public String getCurrentSegment() { return currentSegment; }
  public void setCurrentSegment(String currentSegment) { this.currentSegment = currentSegment; }
  
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
    b.append(id).append(": {");
    b.append("reader=").append(reader).append(", ");
    b.append("startedTime=").append(DateUtil.asCompactDateTime(startedTime)).append(", ");
    b.append("finishedTime=").append(DateUtil.asCompactDateTime(finishedTime)).append(", ");
    b.append("currentSegment=").append(currentSegment).append(", ");
    b.append("currentReadRecordIndex=").append(currentReadRecordIndex).append(", ");
    b.append("currentReadDataPosition=").append(currentReadDataPosition);
    b.append("}");
    return b.toString();
  }
}
