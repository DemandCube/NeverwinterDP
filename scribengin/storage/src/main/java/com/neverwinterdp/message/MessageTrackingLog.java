package com.neverwinterdp.message;

public class MessageTrackingLog {
  private String   name;
  private long     timestamp;
  private short    activityCode;
  private String[] tags;
  
  public MessageTrackingLog() { }
  
  public MessageTrackingLog(String name, long timestamp, short activityCode) {
    this.name         = name;
    this.timestamp    = timestamp;
    this.activityCode = activityCode;
  }
  
  public String getName() { return name; }
  public void setName(String name) { this.name = name; }
  
  public long getTimestamp() { return timestamp; }
  public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
  
  public short getActivityCode() { return activityCode; }
  public void setActivityCode(short activityCode) { this.activityCode = activityCode; }

  public String[] getTags() { return tags; }
  public void setTags(String[] tags) { this.tags = tags; }
}