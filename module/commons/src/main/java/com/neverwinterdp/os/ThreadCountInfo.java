package com.neverwinterdp.os;

import java.lang.management.ThreadMXBean;
import java.util.Date;

import com.neverwinterdp.util.text.DateUtil;
import com.neverwinterdp.util.text.TabularFormater;

public class ThreadCountInfo {
  private Date  timestamp ;
  private String host;
  private long threadStartedCount;
  private long peekThreadCount;
  private long threadCount;
  private long deamonThreadCount;

  public ThreadCountInfo() { }

  public ThreadCountInfo(ThreadMXBean mbean) {
    timestamp = new Date() ;
    threadStartedCount = mbean.getTotalStartedThreadCount();
    peekThreadCount = mbean.getPeakThreadCount();
    threadCount = mbean.getThreadCount();
    deamonThreadCount = mbean.getDaemonThreadCount();
  }

  public Date getTimestamp() { return timestamp; }
  public void setTimestamp(Date timestamp) { this.timestamp = timestamp; }

  public String getHost() { return host; }
  public void setHost(String host) { this.host = host; }

  public long getThreadStartedCount() { return threadStartedCount; }
  public void setThreadStartedCount(long threadStartedCount) { this.threadStartedCount = threadStartedCount; }

  public long getPeekThreadCount() { return peekThreadCount; }
  public void setPeekThreadCount(long peekThreadCount) { this.peekThreadCount = peekThreadCount; }

  public long getThreadCount() { return threadCount; }
  public void setThreadCount(long threadCount) { this.threadCount = threadCount; }

  public long getDeamonThreadCount() { return deamonThreadCount; }
  public void setDeamonThreadCount(long deamonThreadCount) { this.deamonThreadCount = deamonThreadCount; }
  
  public String getFormattedText() { return getFormattedText(this) ; }
  
  static public String getFormattedText(ThreadCountInfo ... tcInfo) {
    String[] header = {"Timestamp", "Host", "Thread Started", "Peek Thread", "# Thread", "# Deamon Thread"} ;
    TabularFormater formatter = new TabularFormater(header) ;
    for(ThreadCountInfo sel : tcInfo) {
      formatter.addRow(
        DateUtil.asCompactDateTime(sel.getTimestamp()),
        sel.getHost(),
        sel.getThreadStartedCount(),
        sel.getPeekThreadCount(),
        sel.getThreadCount(),
        sel.getDeamonThreadCount()
      );
    }
    return formatter.getFormattedText() ;
  }
}