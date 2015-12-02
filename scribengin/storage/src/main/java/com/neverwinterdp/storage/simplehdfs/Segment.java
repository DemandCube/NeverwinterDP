package com.neverwinterdp.storage.simplehdfs;

import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.UUID;

import org.apache.hadoop.fs.FileStatus;
/**
 * The semgnent has the structure: segment-${type}-${yyyyMMddTHHmmss}-${uuid}.data
 * 
 * @author Tuan
 */
public class Segment {
  static public enum Type { 
    large((byte)1), medium((byte)2), small((byte)3), buffer((byte)4);
    
    private byte priority ;
    
    private Type(byte level) {
      this.priority = level ;
    }
    
    public int compare(Type other) {
      if(priority < other.priority) return -1;
      else if(priority > other.priority) return 1;
      else return 0; 
    }
    
    public Type nextLargerType() {
      if(priority == 4) return small ;
      else if(priority == 3) return medium ;
      else if(priority == 2) return large ;
      return null;
    }
  };
  
  static public Comparator<Segment> COMPARATOR = new Comparator<Segment>() {
    @Override
    public int compare(Segment s1, Segment s2) {
      int typeCompare = s1.type.compareTo(s2.type);
      if(typeCompare != 0) return typeCompare;
      return s1.createdTime.compareTo(s2.createdTime);
    }
  };
  
  final static public SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss");
  
  static public long SMALL_DATASIZE_THRESHOLD  = 64 * 1024 * 1024;
  static public long MEDIUM_DATASIZE_THRESHOLD = 16 * SMALL_DATASIZE_THRESHOLD;
  static public long LARGE_DATASIZE_THRESHOLD  = 8  * MEDIUM_DATASIZE_THRESHOLD ;
  
  private Type   type;
  private String createdTime;
  private String uuid;
  private long   dataSize ;

  public Segment() {
    this.type        = Type.buffer;
    this.createdTime = DATE_FORMAT.format(new Date());
    this.uuid        = UUID.randomUUID().toString().replace("-", "");
    this.dataSize    = 0 ;
  }
  
  public Segment(FileStatus fileStatus)  {
    String fileName = fileStatus.getPath().getName();
    int lastDotIdx  = fileName.lastIndexOf('.');
    String name     = fileName.substring(0, lastDotIdx);
    String[] part   = name.split("-");
    type = Type.valueOf(part[1]); 
    createdTime = part[2];
    uuid = part[3];
    this.dataSize = fileStatus.getLen();
  }
  
  public Type getType() { return type; }
  public void setType(Type type) { this.type = type; }

  public String getCreatedTime() { return createdTime; }
  public void   setCreatedTime(String createdTime) { this.createdTime = createdTime; }

  public String getUuid() { return uuid; }
  public void setUuid(String uuid) { this.uuid = uuid; }

  public long getDataSize() { return dataSize; }
  public void setDataSize(long dataSize) { this.dataSize = dataSize; }

  public String toFileName(String ext) {
    StringBuilder b = new StringBuilder();
    b.append("segment").
      append("-").append(type).
      append("-").append(createdTime).
      append("-").append(uuid).
      append(".").append(ext);
    return b.toString();
  }
  
  public String toBufferingPath(String dir) {
    return dir + "/" + toFileName("buffering");
  }
  
  public String toCompletePath(String dir) {
    return dir + "/" + toFileName("complete");
  }
  
  public String toDataPath(String location) {
    return location + "/" + toFileName("data");
  }
  
  static public long getSegmentDataSizeThreshold(Type type) {
    if(type == Type.large) return LARGE_DATASIZE_THRESHOLD;
    else if(type == Type.medium) return MEDIUM_DATASIZE_THRESHOLD;
    return SMALL_DATASIZE_THRESHOLD;
  }
}