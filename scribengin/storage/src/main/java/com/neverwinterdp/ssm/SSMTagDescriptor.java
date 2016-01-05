package com.neverwinterdp.ssm;

public class SSMTagDescriptor {
  private String name;
  private String description;
  private int    segmentId;
  private long   segmentRecordPosition;
  
  public String getName() { return name; }
  public void setName(String name) { this.name = name; }
  
  public String getDescription() { return description; }
  public void setDescription(String description) { this.description = description; }
  
  public int getSegmentId() { return segmentId; }
  public void setSegmentId(int segmentId) { this.segmentId = segmentId; }
  
  public long getSegmentRecordPosition() { return segmentRecordPosition; }
  public void setSegmentRecordPosition(long pos) { segmentRecordPosition = pos; }
  
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append("{");
    b.append("name=").append(name).append(", ");
    b.append("description=").append(description).append(", ");
    b.append("segmentId=").append(segmentId).append(", ");
    b.append("segmentRecordPosition=").append(segmentRecordPosition);
    b.append("}");
    return b.toString();
  }
}
