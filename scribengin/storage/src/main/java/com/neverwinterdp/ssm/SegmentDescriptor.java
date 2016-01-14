package com.neverwinterdp.ssm;

import java.text.DecimalFormat;

import com.neverwinterdp.util.text.DateUtil;

public class SegmentDescriptor {
  static DecimalFormat ID_FORMAT = new DecimalFormat("000000");
  
  static public enum Status { Writing, WritingComplete, Complete }
  
  private int    id ;
  private String segmentId;
  private String writer;
  private String writerId;
  private long   createdTime;
  private long   finishedTime = -1l;
  private Status status = Status.Writing;

  private long   recordFrom = -1l;
  private long   recordTo   = -1l;
  
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

  public String getWriter() { return writer; }
  public void setWriter(String creator) { this.writer = creator; }

  public String getWriterId() { return writerId; }
  public void setWriterId(String creatorId) { this.writerId = creatorId; }

  public long getCreatedTime() { return createdTime; }
  public void setCreatedTime(long createdTime) { this.createdTime = createdTime; }

  public long getFinishedTime() { return finishedTime;}
  public void setFinishedTime(long finishedTime) { this.finishedTime = finishedTime; }

  public Status getStatus() { return status; }
  public void setStatus(Status status) { this.status = status; }

  public long getRecordFrom() { return recordFrom; }
  public void setRecordFrom(long from) { this.recordFrom = from; }

  public long getRecordTo() { return recordTo; }
  public void setRecordTo(long to) { this.recordTo = to; }

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
    b.append("creator=").append(writer).append(", ");
    b.append("createdTime=").append(DateUtil.asCompactDateTime(createdTime)).append(", ");
    b.append("finishedTime=").append(DateUtil.asCompactDateTime(finishedTime)).append(", ");
    b.append("status=").append(status).append(", ");
    b.append("from=").append(recordFrom).append(", ");
    b.append("to=").append(recordTo).append(", ");
    b.append("dataSegmentNumOfRecords=").append(dataSegmentNumOfRecords).append(", ");
    b.append("dataSegmentLastCommitPos=").append(dataSegmentLastCommitPos).append(", ");
    b.append("dataSegmentCommitCount=").append(dataSegmentCommitCount);
    b.append("}");
    return b.toString();
  }
  
  static public String toSegmentId(int id) {
    return "segment-" + ID_FORMAT.format(id);
  }
  
  static public int extractId(String segmentId) {
    return Integer.parseInt(segmentId.substring("segment-".length() + 1));
  }
}