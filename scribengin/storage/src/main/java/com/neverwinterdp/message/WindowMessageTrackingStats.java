package com.neverwinterdp.message;

import java.util.HashMap;
import java.util.Map;

public class WindowMessageTrackingStats {
  private int  fromWindowId = -1;
  private int  toWindowId   = -1;
  private long trackingCount ;
  private long trackingLostCount;
  private long trackingDuplicatedCount;
  private Map<String, WindowMessageTrackingLogStat> logStats = new HashMap<>();
  
  public int getFromWindowId() { return fromWindowId;}
  public void setFromWindowId(int fromWindowId) {
    this.fromWindowId = fromWindowId;
  }

  public int getToWindowId() { return toWindowId; }
  public void setToWindowId(int toWindowId) {
    this.toWindowId = toWindowId;
  }

  public long getTrackingCount() { return trackingCount; }
  public void setTrackingCount(long trackingCount) {
    this.trackingCount = trackingCount;
  }
  
  public long getTrackingLostCount() { return trackingLostCount; }
  public void setTrackingLostCount(long trackingLostCount) {
    this.trackingLostCount = trackingLostCount;
  }

  public long getTrackingDuplicatedCount() { return trackingDuplicatedCount; }
  public void setTrackingDuplicatedCount(long trackingDuplicatedCount) {
    this.trackingDuplicatedCount = trackingDuplicatedCount;
  }
  
  public Map<String, WindowMessageTrackingLogStat> getLogStats() { return logStats; }
  public void setLogStats(Map<String, WindowMessageTrackingLogStat> logStats) { this.logStats = logStats; }
  
  public long getLogNameCount(String logName) {
    WindowMessageTrackingLogStat logChunkStat = logStats.get(logName);
    if(logChunkStat == null) return 0;
    return logChunkStat.getCount() ;
  }
  
  public void merge(WindowMessageTrackingStat otherChunk) {
    if(fromWindowId < 0) fromWindowId = otherChunk.getWindowId();
    toWindowId = otherChunk.getWindowId();
    trackingCount +=  otherChunk.getTrackingCount();
    trackingLostCount += otherChunk.getTrackingLostCount();
    trackingDuplicatedCount += otherChunk.getTrackingDuplicatedCount();
    Map<String, WindowMessageTrackingLogStat> otherLogStats = otherChunk.getLogStats();
    for(String otherLogName : otherLogStats.keySet()) {
      WindowMessageTrackingLogStat logStat = logStats.get(otherLogName);
      if(logStat == null) {
        logStat = new WindowMessageTrackingLogStat();
        logStats.put(otherLogName, logStat);
      }
      logStat.merge(otherLogStats.get(otherLogName));
    }
  }
  
  public void merge(WindowMessageTrackingStats other) {
    toWindowId = other.getToWindowId();
    trackingCount +=  other.getTrackingCount();
    trackingLostCount += other.getTrackingLostCount();
    trackingDuplicatedCount += other.getTrackingDuplicatedCount();
    
    Map<String, WindowMessageTrackingLogStat> otherLogStats = other.getLogStats();
    for(String otherLogName : otherLogStats.keySet()) {
      WindowMessageTrackingLogStat logStat = logStats.get(otherLogName);
      if(logStat == null) {
        logStat = new WindowMessageTrackingLogStat();
        logStats.put(otherLogName, logStat);
      }
      logStat.merge(otherLogStats.get(otherLogName));
    }
  }
}