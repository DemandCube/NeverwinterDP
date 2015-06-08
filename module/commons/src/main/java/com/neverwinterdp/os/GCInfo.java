package com.neverwinterdp.os;

import java.lang.management.GarbageCollectorMXBean;
import java.util.Date;

import com.neverwinterdp.util.text.DateUtil;
import com.neverwinterdp.util.text.StringUtil;
import com.neverwinterdp.util.text.TabularFormater;

public class GCInfo {
  private String name;
  private Date   timestamp;
  private long collectionCount;
  private long collectionTime;
  private String poolNames;

  public GCInfo() { }
  
  public GCInfo(GarbageCollectorMXBean gcbean) {
    name = gcbean.getName();
    timestamp = new Date(System.currentTimeMillis()) ;
    collectionCount = gcbean.getCollectionCount();
    collectionTime =  gcbean.getCollectionTime();
    poolNames = StringUtil.joinStringArray(gcbean.getMemoryPoolNames(), "|");
  }
  
  public String getName() { return name; }
  public void setName(String name) { this.name = name;}

  public Date getTimestamp() { return timestamp; }
  public void setTimestamp(Date timestamp) {  this.timestamp = timestamp; }

  public long getCollectionCount() { return collectionCount; }
  public void setCollectionCount(long collectionCount) { this.collectionCount = collectionCount; }

  public long getCollectionTime() { return collectionTime; }
  public void setCollectionTime(long collectionTime) { this.collectionTime = collectionTime;}

  public String getPoolNames() { return poolNames; }
  public void setPoolNames(String poolNames) { this.poolNames = poolNames; }
  
  static public String getFormattedText(GCInfo ... gcInfo) {
    String[] header = {"Name", "Timestamp", "Collection Count", "Collection Time", "Pool Names"} ;
    TabularFormater formatter = new TabularFormater(header) ;
    for(GCInfo sel : gcInfo) {
      formatter.addRow(
          sel.getName(), 
          DateUtil.asCompactDateTime(sel.getTimestamp()),
          sel.getCollectionCount(), 
          DateUtil.timeMillisToHumanReadable(sel.getCollectionTime()), 
          sel.getPoolNames());
    }
    return formatter.getFormattedText() ;
  }
}
