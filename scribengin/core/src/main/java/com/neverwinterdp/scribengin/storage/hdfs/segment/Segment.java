package com.neverwinterdp.scribengin.storage.hdfs.segment;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.vm.environment.yarn.HDFSUtil;
/**
 * The semgnent has the structure: segment-${type}-${yyyyMMddTHHmmss}-${uuid}.data
 * 
 * @author Tuan
 */
public class Segment {
  static public enum Type { buffer, small, medium, large};
  
  final static public SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss");
  
  final static public long SMALL_DATASIZE_THRESHOLD  = 32 * 1024;
  final static public long MEDIUM_DATASIZE_THRESHOLD = SMALL_DATASIZE_THRESHOLD  * 4;
  final static public long LARGE_DATASIZE_THRESHOLD  = MEDIUM_DATASIZE_THRESHOLD * 8;
  
  private Type   type;
  private Date   createdTime;
  private String uuid;
  private long   dataSize ;

  public Segment() {
    this.type        = Type.buffer;
    this.createdTime = new Date();
    this.uuid        = UUID.randomUUID().toString().replace("-", "");
    this.dataSize    = 0 ;
  }
  
  public Segment(FileStatus fileStatus) throws ParseException {
    String fileName = fileStatus.getPath().getName();
    int lastDotIdx = fileName.lastIndexOf('.');
    String name = fileName.substring(0, lastDotIdx);
    String[] part = name.split("-");
    type = Type.valueOf(part[1]); 
    createdTime = DATE_FORMAT.parse(part[2]);
    uuid = part[3];
    this.dataSize = fileStatus.getLen();
  }
  
  public Type getType() { return type; }
  public void setType(Type type) { this.type = type; }

  public Date getCreatedTime() { return createdTime; }
  public void setCreatedTime(Date createdTime) { this.createdTime = createdTime; }

  public String getUuid() { return uuid; }
  public void setUuid(String uuid) { this.uuid = uuid; }

  public long getDataSize() { return dataSize; }
  public void setDataSize(long dataSize) { this.dataSize = dataSize; }

  public String toFileName(String ext) {
    StringBuilder b = new StringBuilder();
    b.append("segment").
      append("-").append(type).
      append("-").append(DATE_FORMAT.format(createdTime)).
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
}