package com.neverwinterdp.os;

import java.io.Serializable;
import java.lang.management.GarbageCollectorMXBean;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.neverwinterdp.util.text.DateUtil;
import com.neverwinterdp.util.text.StringUtil;
import com.neverwinterdp.util.text.TabularFormater;

@SuppressWarnings("serial")
public class GCInfo implements Serializable {
  @JsonFormat(shape=JsonFormat.Shape.STRING, pattern="dd/MM/yyyy HH:mm:ss")
  private Date   timestamp;
  private String host;
  private String name;
  private long collectionCount;
  private long collectionTime;
  private String poolNames;

  public GCInfo() { }
  
  public GCInfo(GarbageCollectorMXBean gcbean) {
    timestamp = new Date(System.currentTimeMillis()) ;
    name = gcbean.getName();
    collectionCount = gcbean.getCollectionCount();
    collectionTime =  gcbean.getCollectionTime();
    poolNames = StringUtil.joinStringArray(gcbean.getMemoryPoolNames(), "|");
  }
  
  public String uniqueId() { 
    return "host=" + host + ",timestamp=" + DateUtil.asCompactDateTimeId(timestamp); 
  }
  
  public Date getTimestamp() { return timestamp; }
  public void setTimestamp(Date timestamp) {  this.timestamp = timestamp; }
  
  public String getHost() { return host; }
  public void setHost(String host) { this.host = host; }

  public String getName() { return name; }
  public void setName(String name) { this.name = name;}
  
  public long getCollectionCount() { return collectionCount; }
  public void setCollectionCount(long collectionCount) { this.collectionCount = collectionCount; }

  public long getCollectionTime() { return collectionTime; }
  public void setCollectionTime(long collectionTime) { this.collectionTime = collectionTime;}

  public String getPoolNames() { return poolNames; }
  public void setPoolNames(String poolNames) { this.poolNames = poolNames; }
  
  static public String getFormattedText(GCInfo ... gcInfo) {
    String[] header = {"Name","Host", "Timestamp", "Collection Count", "Collection Time", "Pool Names"} ;
    TabularFormater formatter = new TabularFormater(header) ;
    for(GCInfo sel : gcInfo) {
      formatter.addRow(
          DateUtil.asCompactDateTime(sel.getTimestamp()),
          sel.getHost(),
          sel.getName(), 
          sel.getCollectionCount(), 
          DateUtil.timeMillisToHumanReadable(sel.getCollectionTime()), 
          sel.getPoolNames());
    }
    return formatter.getFormattedText() ;
  }
}
