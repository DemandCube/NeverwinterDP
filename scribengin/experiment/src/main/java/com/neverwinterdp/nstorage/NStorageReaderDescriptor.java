package com.neverwinterdp.nstorage;

import java.text.DecimalFormat;

import com.neverwinterdp.util.text.DateUtil;

public class NStorageReaderDescriptor {
  static DecimalFormat ID_FORMAT = new DecimalFormat("000000");
  
  private String       id;
  private String       reader;
  private long         startedTime;
  private long         finishedTime;
  
  
  public NStorageReaderDescriptor() { }
  
  public NStorageReaderDescriptor(int id, String reader) {
    this.id          = reader + "-" + ID_FORMAT.format(id) ;
    this.reader      = reader;
    this.startedTime = System.currentTimeMillis();
  }
  
  public String getId() { return id; }
  public void setId(String id) { this.id = id; }
  
  public String getReader() { return reader; }
  public void setReader(String reader) { this.reader = reader; }
  
  public long getStartedTime() { return startedTime; }
  public void setStartedTime(long startedTime) { this.startedTime = startedTime; }
  
  public long getFinishedTime() { return finishedTime;}
  public void setFinishedTime(long finishedTime) { this.finishedTime = finishedTime; }
  
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append(id).append(": {");
    b.append("reader=").append(reader).append(", ");
    b.append("startedTime=").append(DateUtil.asCompactDateTime(startedTime)).append(", ");
    b.append("finishedTime=").append(DateUtil.asCompactDateTime(finishedTime));
    b.append("}");
    return b.toString();
  }
}
