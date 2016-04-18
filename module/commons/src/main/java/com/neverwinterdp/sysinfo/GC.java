package com.neverwinterdp.sysinfo;

import java.io.Serializable;
import java.lang.management.GarbageCollectorMXBean;

@SuppressWarnings("serial")
public class GC implements Serializable {
  private String name;
  private long   collectionCount;
  private long   diffCollectionCount;

  public GC() { }
  
  public GC(GarbageCollectorMXBean gcbean) {
    name = gcbean.getName();
    collectionCount = gcbean.getCollectionCount();
  }
  
  public String getName() { return name; }
  public void setName(String name) { this.name = name;}
  
  public long getCollectionCount() { return collectionCount; }
  public void setCollectionCount(long collectionCount) { this.collectionCount = collectionCount; }

  public long getDiffCollectionCount() { return diffCollectionCount; }
  public void setDiffCollectionCount(long diffCollectionCount) { this.diffCollectionCount = diffCollectionCount; }
}
