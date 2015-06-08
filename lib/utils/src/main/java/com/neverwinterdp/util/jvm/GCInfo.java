package com.neverwinterdp.util.jvm;

import java.io.Serializable;
import java.lang.management.GarbageCollectorMXBean;

import com.neverwinterdp.util.text.StringUtil;

public class GCInfo implements Serializable {
  private String name;
  private long collectionCount;
  private long collectionTime;
  private String poolNames;

  public GCInfo() {
  }
  
  public GCInfo(GarbageCollectorMXBean gcbean) {
    name = gcbean.getName();
    collectionCount = gcbean.getCollectionCount();
    collectionTime =  gcbean.getCollectionTime();
    poolNames = StringUtil.joinStringArray(gcbean.getMemoryPoolNames(), "|");
  }
  
  public String getName() { return name; }
  public void setName(String name) { this.name = name;}

  public long getCollectionCount() { return collectionCount; }
  public void setCollectionCount(long collectionCount) { this.collectionCount = collectionCount; }

  public long getCollectionTime() { return collectionTime; }
  public void setCollectionTime(long collectionTime) { this.collectionTime = collectionTime;}

  public String getPoolNames() { return poolNames; }
  public void setPoolNames(String poolNames) { this.poolNames = poolNames; }
}
