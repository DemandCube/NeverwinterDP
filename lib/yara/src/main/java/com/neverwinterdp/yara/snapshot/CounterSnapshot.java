package com.neverwinterdp.yara.snapshot;

import java.io.Serializable;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.neverwinterdp.util.text.DateUtil;
import com.neverwinterdp.yara.Counter;

public class CounterSnapshot implements Serializable {
  @JsonFormat(shape=JsonFormat.Shape.STRING, pattern="dd/MM/yyyy HH:mm:ss")
  private Date   timestamp;
  private String serverName;
  private String name ;
  private long   count ;
  
  public CounterSnapshot() {
  }
  
  public CounterSnapshot(String serverName, Counter counter) {
    timestamp = new Date();
    this.serverName = serverName;
    this.name = counter.getName();
    this.count = counter.getCount();
  }

  public String uniqueId() { 
    return "host=" + serverName + ",timestamp=" + DateUtil.asCompactDateTimeId(timestamp); 
  }
  
  public Date getTimestamp() { return timestamp; }
  public void setTimestamp(Date timestamp) { this.timestamp = timestamp; }

  public String getServerName() { return serverName; }
  public void setServerName(String serverName) { this.serverName = serverName; }

  public String getName() { return name; }
  public void setName(String name) { this.name = name; }

  public long getCount() { return count; }
  public void setCount(long count) { this.count = count; }
}
