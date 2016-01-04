package com.neverwinterdp.ssm;

public class SegmentTag {
  private String name;
  private String description;
  private int    segmentId;
  private long   recordPosition;
  
  public String getName() { return name; }
  public void setName(String name) { this.name = name; }
  
  public String getDescription() { return description; }
  public void setDescription(String description) { this.description = description; }
  
  public int getSegmentId() { return segmentId; }
  public void setSegmentId(int segmentId) { this.segmentId = segmentId; }
  
  public long getRecordPosition() { return recordPosition; }
  public void setRecordPosition(long recordPosition) { this.recordPosition = recordPosition; }
}
