package com.neverwinterdp.os;

import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Date;

import com.neverwinterdp.util.ExceptionUtil;
import com.neverwinterdp.util.text.DateUtil;
import com.neverwinterdp.util.text.TabularFormater;

public class DetailThreadInfo {
  private Date   timestamp ;
  private String host ;
  private long   id;
  private String name;
  private String state;
  private long   blockCount;
  private long   blockTime;
  private long   waitedCount;
  private long   waitedTime;
  private long   cpuTime;
  private long   userTime;
  private String stackTrace;
  
  public DetailThreadInfo() { } 
  
  public DetailThreadInfo(ThreadMXBean mbean, ThreadInfo tinfo) {
    timestamp   = new Date();
    id          = tinfo.getThreadId();
    name        = tinfo.getThreadName();
    state       = tinfo.getThreadState().toString();
    blockCount  = tinfo.getBlockedCount();
    blockTime   = tinfo.getBlockedTime();
    waitedCount = tinfo.getWaitedCount();
    waitedTime  = tinfo.getWaitedTime();
    cpuTime     = mbean.getThreadCpuTime(id);
    userTime    = mbean.getThreadUserTime(id);
    stackTrace  = ExceptionUtil.toString(tinfo.getStackTrace());
  }

  public Date getTimestamp() { return timestamp; }
  public void setTimestamp(Date timestamp) { this.timestamp = timestamp; }
  
  public String getHost() { return host; }
  public void setHost(String host) { this.host = host; }

  public long getId() { return id; }
  public void setId(long id) { this.id = id; }

  public String getName() { return name; }
  public void setName(String name) { this.name = name; }

  public String getState() { return state; }
  public void setState(String state) { this.state = state;}

  public long getBlockCount() { return blockCount; }
  public void setBlockCount(long blockCount) { this.blockCount = blockCount; }

  public long getBlockTime() { return blockTime; }
  public void setBlockTime(long blockTime) { this.blockTime = blockTime; }

  public long getWaitedCount() { return waitedCount; }
  public void setWaitedCount(long waitedCount) { this.waitedCount = waitedCount; }

  public long getWaitedTime() { return waitedTime; }
  public void setWaitedTime(long waitedTime) { this.waitedTime = waitedTime; }

  public long getCpuTime() { return cpuTime; }
  public void setCpuTime(long cpuTime) { this.cpuTime = cpuTime; }

  public long getUserTime() { return userTime; }
  public void setUserTime(long userTime) { this.userTime = userTime; }

  public String getStackTrace() { return stackTrace; }
  public void setStackTrace(String stackTrace) { this.stackTrace = stackTrace; }

  public String getFormattedText() {
    return getFormattedText(this) ;
  }
  
  static public String getFormattedText(DetailThreadInfo ... info) {
    String[] header = { 
      "Timestamp", "Host", "Id", "Name", "State", "Block Count", "Block Time", "Waited Count", "Waited Time", "CPU Time", "User Time" 
    };
    TabularFormater formatter = new TabularFormater(header) ;
    for(DetailThreadInfo sel : info) {
      formatter.addRow(
        DateUtil.asCompactDateTime(sel.getTimestamp()),
        sel.getHost(),
        sel.getId(),
        sel.getName(),
        sel.getState(),
        sel.getBlockCount(),
        sel.getBlockTime(),
        sel.getWaitedCount(),
        sel.getWaitedTime(),
        sel.getCpuTime(),
        sel.getUserTime()
      );
    }
    return formatter.getFormattedText() ;
  }
}