package com.neverwinterdp.message;

import java.util.HashMap;
import java.util.Map;

public class AggregateMessageTrackingChunkStat {
  private int  fromChunkId = -1;
  private int  toChunkId   = -1;
  private long trackingCount ;
  private long trackingLostCount;
  private long trackingDuplicatedCount;
  private Map<String, MessageTrackingLogChunkStat> logStats = new HashMap<>();
  
  public int getFromChunkId() { return fromChunkId;}
  public void setFromChunkId(int fromChunkId) {
    this.fromChunkId = fromChunkId;
  }

  public int getToChunkId() { return toChunkId; }
  public void setToChunkId(int toChunkId) {
    this.toChunkId = toChunkId;
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
  
  public Map<String, MessageTrackingLogChunkStat> getLogStats() { return logStats; }
  public void setLogStats(Map<String, MessageTrackingLogChunkStat> logStats) { this.logStats = logStats; }
  
  public long getLogNameCount(String logName) {
    MessageTrackingLogChunkStat logChunkStat = logStats.get(logName);
    if(logChunkStat == null) return 0;
    return logChunkStat.getCount() ;
  }
  
  public void merge(MessageTrackingChunkStat otherChunk) {
    if(fromChunkId < 0) fromChunkId = otherChunk.getChunkId();
    toChunkId = otherChunk.getChunkId();
    trackingCount +=  otherChunk.getTrackingCount();
    trackingLostCount += otherChunk.getTrackingLostCount();
    trackingDuplicatedCount += otherChunk.getTrackingDuplicatedCount();
    Map<String, MessageTrackingLogChunkStat> otherLogStats = otherChunk.getLogStats();
    for(String otherLogName : otherLogStats.keySet()) {
      MessageTrackingLogChunkStat logStat = logStats.get(otherLogName);
      if(logStat == null) {
        logStat = new MessageTrackingLogChunkStat();
        logStats.put(otherLogName, logStat);
      }
      logStat.merge(otherLogStats.get(otherLogName));
    }
  }
  
  public void merge(AggregateMessageTrackingChunkStat other) {
    toChunkId = other.getToChunkId();
    trackingCount +=  other.getTrackingCount();
    trackingLostCount += other.getTrackingLostCount();
    trackingDuplicatedCount += other.getTrackingDuplicatedCount();
    
    Map<String, MessageTrackingLogChunkStat> otherLogStats = other.getLogStats();
    for(String otherLogName : otherLogStats.keySet()) {
      MessageTrackingLogChunkStat logStat = logStats.get(otherLogName);
      if(logStat == null) {
        logStat = new MessageTrackingLogChunkStat();
        logStats.put(otherLogName, logStat);
      }
      logStat.merge(otherLogStats.get(otherLogName));
    }
  }
}