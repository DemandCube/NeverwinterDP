package com.neverwinterdp.nstorage.segment;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.util.text.DateUtil;

public class WriterDescriptor {
  static DecimalFormat ID_FORMAT = new DecimalFormat("000000");
  
  private String       id;
  private String       writer;
  private long         startedTime;
  private long         finishedTime;
  private String       currentSegment;
  private List<String> logs;
  
  public WriterDescriptor() { }
  
  public WriterDescriptor(int id, String writer) {
    this.id          = writer + "-" + ID_FORMAT.format(id) ;
    this.writer      = writer;
    this.startedTime = System.currentTimeMillis();
  }
  
  public String getId() { return id; }
  public void setId(String id) { this.id = id; }
  
  public String getWriter() { return writer; }
  public void setWriter(String writer) { this.writer = writer; }

  public long getStartedTime() { return startedTime; }
  public void setStartedTime(long startedTime) { this.startedTime = startedTime; }
  
  public long getFinishedTime() { return finishedTime;}
  public void setFinishedTime(long finishedTime) { this.finishedTime = finishedTime; }
  
  public String getCurrentSegment() { return currentSegment; }
  public void setCurrentSegment(String currentSegment) { this.currentSegment = currentSegment; }
  
  public List<String> getLogs() { return logs; }
  public void setLogs(List<String> logs) {  this.logs = logs; }
  
  public void addLog(String log) {
    if(logs == null) logs = new ArrayList<>();
    logs.add(log);
  }
  
  public void logStartSegment(String segmentName) {
    currentSegment = segmentName;  
    addLog("Start  writing the segment " + currentSegment + " at " + DateUtil.asCompactDateTime(System.currentTimeMillis()));
  }
  
  public void logFinishSegment() {
    if(currentSegment != null) {
      addLog("Finish writing the segment " + currentSegment + " at " + DateUtil.asCompactDateTime(System.currentTimeMillis()));
      currentSegment = null;
    }
  }
  
  public String basicInfo() {
    StringBuilder b = new StringBuilder();
    b.append("writer=").append(writer).append(", ");
    b.append("startedTime=").append(DateUtil.asCompactDateTime(startedTime)).append(", ");
    b.append("finishedTime=").append(DateUtil.asCompactDateTime(finishedTime)).append(", ");
    b.append("currentSegment=").append(currentSegment);
    return b.toString();
  }
  
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append(id).append(": {").append(basicInfo()).append("}");
    return b.toString();
  }
}
