package com.neverwinterdp.nstorage.segment;

import java.text.DecimalFormat;

public class DataSegmentDescriptor {
  static DecimalFormat ID_FORMAT = new DecimalFormat("000");
  
  static public enum Status { WRITING, COMPLETE }
  
  private int    id;
  private String name;
  private String creator;
  private long   lastCommitRecordIndex;
  private long   lastCommitPos;
  private long   createdTime;
  private long   finishedTime;
  private String location;
  private Status status = Status.WRITING;
  
  public DataSegmentDescriptor() {
  }

  public DataSegmentDescriptor(int id, String creator) {
    this.id          = id ;
    this.name        = toName(id);
    this.creator     = creator;
    this.createdTime = System.currentTimeMillis();
  }
  
  public int getId() { return id; }
  public void setId(int id) { this.id = id; }

  public String getName() { return name; }
  public void setName(String name) { this.name = name; }

  public String getCreator() { return creator; }
  public void setCreator(String creator) { this.creator = creator; }
  
  public long getLastCommitRecordIndex() { return lastCommitRecordIndex; }
  
  public void setLastCommitRecordIndex(long lastCommitRecordIndex) { 
    this.lastCommitRecordIndex = lastCommitRecordIndex;
  }
  
  public long getLastCommitPos() { return lastCommitPos; }
  public void setLastCommitPos(long lastCommitPos) {
    this.lastCommitPos = lastCommitPos;
  }
  
  public long getCreatedTime() { return createdTime; }
  public void setCreatedTime(long createdTime) {
    this.createdTime = createdTime;
  }
  
  public long getFinishedTime() { return finishedTime; }
  public void setFinishedTime(long finishedTime) {
    this.finishedTime = finishedTime;
  }
  
  public String getLocation() { return location; }
  public void setLocation(String location) {
    this.location = location;
  }
  
  public Status getStatus() { return status; }
  public void setStatus(Status status) {
    this.status = status;
  }
  
  static public String toName(int id) {
    return "segment-data-" + ID_FORMAT.format(id);
  }
}
