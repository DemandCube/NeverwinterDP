package com.neverwinterdp.nstorage.segment;

import java.text.DecimalFormat;

public class SegmentDescriptor {
  static DecimalFormat ID_FORMAT = new DecimalFormat("000000");
  static public enum Status { WRITING, OPTIMIZING, COMPLETE }
  
  private int    id ;
  private String name;
  private Status status = Status.WRITING;
  private long   fromRecordIndex;
  private long   toRecordIndex;

  private long   createdTime;
  private long   optimizedTime ;
  private long   completeTime;
  
  private long   writtenDuration;
  private long   optimizedDuration;
  
  public SegmentDescriptor() {}
  
  public SegmentDescriptor(int id) {
    this.id          = id ;
    this.name        = toSegmentName(id);
    this.createdTime = System.currentTimeMillis();
  }
  
  public int getId() { return id; }
  public void setId(int id) { this.id = id; }
  
  public String getName() { return name; }
  public void setName(String name) { this.name = name; }
  
  public Status getStatus() { return status; }
  public void setStatus(Status status) { this.status = status;}
  
  public long getFromRecordIndex() { return fromRecordIndex; }
  public void setFromRecordIndex(long fromRecordIndex) { this.fromRecordIndex = fromRecordIndex; }
  
  public long getToRecordIndex() { return toRecordIndex; }
  public void setToRecordIndex(long toRecordIndex) { this.toRecordIndex = toRecordIndex; }
  
  public long getCreatedTime() { return createdTime;}
  public void setCreatedTime(long createdTime) { this.createdTime = createdTime; }
  
  public long getOptimizedTime() { return optimizedTime; }
  public void setOptimizedTime(long optimizedTime) {
    this.optimizedTime = optimizedTime;
  }
  
  public long getCompleteTime() { return completeTime; }
  public void setCompleteTime(long completeTime) {
    this.completeTime = completeTime;
  }
  
  public long getWrittenDuration() { return writtenDuration; }
  public void setWrittenDuration(long writtenDuration) {
    this.writtenDuration = writtenDuration;
  }
  
  public long getOptimizedDuration() { return optimizedDuration; }
  public void setOptimizedDuration(long optimizedDuration) {
    this.optimizedDuration = optimizedDuration;
  }
  
  static public String toSegmentName(int id) {
    return "segment-" + ID_FORMAT.format(id);
  }
}