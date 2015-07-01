package com.neverwinterdp.yara;

public class CounterInfo {
  private String serverName;
  private String name ;
  private long   count ;
  
  public CounterInfo() {
  }
  
  public CounterInfo(String serverName, Counter counter) {
    this.serverName = serverName;
    this.name = counter.getName();
    this.count = counter.getCount();
  }

  public String getServerName() { return serverName; }
  public void setServerName(String serverName) { this.serverName = serverName; }

  public String getName() { return name; }
  public void setName(String name) { this.name = name; }

  public long getCount() { return count; }
  public void setCount(long count) { this.count = count; }
}
