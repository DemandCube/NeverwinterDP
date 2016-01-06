package com.neverwinterdp.message;

public class MessageTrackingLog {
  private long     timestamp;
  private String   name;
  private String[] tag;

  public MessageTrackingLog() { }
  
  public MessageTrackingLog(String name, String[] tag) {
    this.timestamp = System.currentTimeMillis();
    this.name      = name;
    this.tag       = tag;
  }
  
  public long getTimestamp() { return timestamp; }
  public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
  
  public String getName() { return name; }
  public void setName(String name) { this.name = name; }

  public String[] getTag() { return tag; }
  public void setTag(String[] tag) { this.tag = tag; }
}