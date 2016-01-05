package com.neverwinterdp.ssm;

import com.neverwinterdp.util.text.DateUtil;

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
  
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append("{");
    b.append("name=").append(name).append(", ");
    b.append("description=").append(description).append(", ");
    b.append("segmentId=").append(segmentId).append(", ");
    b.append("recordPosition=").append(recordPosition);
    b.append("}");
    return b.toString();
  }
}
