package com.neverwinterdp.os;

import java.io.Serializable;
import java.lang.management.ClassLoadingMXBean;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.neverwinterdp.util.text.DateUtil;
import com.neverwinterdp.util.text.TabularFormater;

@SuppressWarnings("serial")
public class ClassLoadedInfo implements Serializable {
  @JsonFormat(shape=JsonFormat.Shape.STRING, pattern="dd/MM/yyyy HH:mm:ss")
  private Date   timestamp;
  private String host;
  private int    loadedClassCount;
  private long   totalLoadedClassCount;
  private long   unloadedClassCount;

 
  public ClassLoadedInfo() { }
  
  public ClassLoadedInfo(ClassLoadingMXBean clbean) {
    timestamp             = new Date(System.currentTimeMillis()) ;
    totalLoadedClassCount = clbean.getTotalLoadedClassCount();
    loadedClassCount      = clbean.getLoadedClassCount();
    unloadedClassCount    = clbean.getUnloadedClassCount();
  }
  
  public String uniqueId() { 
    return "host=" + host + ",timestamp=" + DateUtil.asCompactDateTimeId(timestamp); 
  }
  
  public Date getTimestamp() { return timestamp; }
  public void setTimestamp(Date timestamp) {  this.timestamp = timestamp; }
  
  public String getHost() { return host; }
  public void setHost(String host) { this.host = host; }

  public int getLoadedClassCount() { return loadedClassCount; }
  public void setLoadedClassCount(int loadedClassCount) {
    this.loadedClassCount = loadedClassCount;
  }

  public long getTotalLoadedClassCount() { return totalLoadedClassCount; }
  public void setTotalLoadedClassCount(long totalLoadedClassCount) {
    this.totalLoadedClassCount = totalLoadedClassCount;
  }

  public long getUnloadedClassCount() { return unloadedClassCount; }
  public void setUnloadedClassCount(long unloadedClassCount) {
    this.unloadedClassCount = unloadedClassCount;
  }
  
  static public String getFormattedText(ClassLoadedInfo ... clInfo) {
    String[] header = {"Host", "Timestamp", "Total Loaded Class Count", "Loaded Class Count", "Unloaded Class Count"} ;
    TabularFormater formatter = new TabularFormater(header) ;
    for(ClassLoadedInfo sel : clInfo) {
      formatter.addRow(
          sel.getHost(),
          DateUtil.asCompactDateTime(sel.getTimestamp()),
          sel.getTotalLoadedClassCount(), 
          sel.getLoadedClassCount(),
          sel.getUnloadedClassCount());
    }
    return formatter.getFormattedText() ;
  }
}
