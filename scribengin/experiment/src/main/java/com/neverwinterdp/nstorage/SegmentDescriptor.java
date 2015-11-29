package com.neverwinterdp.nstorage;

import java.text.DecimalFormat;

import com.neverwinterdp.util.text.DateUtil;

public class SegmentDescriptor {
  static DecimalFormat ID_FORMAT = new DecimalFormat("000000");
  
  static public enum Status { WRITING, COMPLETE }
  
  private int    id ;
  private String segmentId;
  private String creator;
  private long   createdTime;
  private long   finishedTime = -1l;
  private Status status = Status.WRITING;

  private long   from = -1l;
  private long   to   = -1l;
  
  private long   dataSegmentNumOfRecords;
  private long   dataSegmentLastCommitPos;
  private int    dataSegmentCommitCount;
  
  public SegmentDescriptor() {}
  
  public SegmentDescriptor(int id) {
    this.id          = id ;
    this.segmentId   = toSegmentId(id);
    this.createdTime = System.currentTimeMillis();
  }
  
  public int getId() { return id; }
  public void setId(int id) { this.id = id; }

  public String getSegmentId() { return segmentId; }
  public void   setSegmentId(String name) { this.segmentId = name; }

  public String getCreator() { return creator; }
  public void setCreator(String creator) { this.creator = creator; }

  public long getCreatedTime() { return createdTime; }
  public void setCreatedTime(long createdTime) { this.createdTime = createdTime; }

  public long getFinishedTime() { return finishedTime;}
  public void setFinishedTime(long finishedTime) { this.finishedTime = finishedTime; }

  public Status getStatus() { return status; }
  public void setStatus(Status status) { this.status = status; }

  public long getFrom() { return from; }
  public void setFrom(long from) { this.from = from; }

  public long getTo() { return to; }
  public void setTo(long to) { this.to = to; }

  public long getDataSegmentNumOfRecords() { return dataSegmentNumOfRecords; }
  public void setDataSegmentNumOfRecords(long dataSegmentNumOfRecords) {
    this.dataSegmentNumOfRecords = dataSegmentNumOfRecords;
  }

  public long getDataSegmentLastCommitPos() { return dataSegmentLastCommitPos; }
  public void setDataSegmentLastCommitPos(long dataSegmentLastCommitPos) {
    this.dataSegmentLastCommitPos = dataSegmentLastCommitPos;
  }
  
  public int getDataSegmentCommitCount() { return dataSegmentCommitCount; }
  public void setDataSegmentCommitCount(int dataSegmentCommitCount) {
    this.dataSegmentCommitCount = dataSegmentCommitCount;
  }

  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append(segmentId).append(": {");
    b.append("id=").append(id).append(", ");
    b.append("creator=").append(creator).append(", ");
    b.append("createdTime=").append(DateUtil.asCompactDateTime(createdTime)).append(", ");
    b.append("finishedTime=").append(DateUtil.asCompactDateTime(finishedTime)).append(", ");
    b.append("status=").append(status).append(", ");
    b.append("from=").append(from).append(", ");
    b.append("to=").append(to).append(", ");
    b.append("dataSegmentNumOfRecords=").append(dataSegmentNumOfRecords).append(", ");
    b.append("dataSegmentLastCommitPos=").append(dataSegmentLastCommitPos).append(", ");
    b.append("dataSegmentCommitCount=").append(dataSegmentCommitCount);
    b.append("}");
    return b.toString();
  }
  
  static public String toSegmentId(int id) {
    return "segment-" + ID_FORMAT.format(id);
  }
}