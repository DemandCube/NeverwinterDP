package com.neverwinterdp.message;

import java.util.HashMap;
import java.util.Map;

public class AggregateMessageTrackingChunkStat {
  private int  fromChunkId = -1;
  private int  toChunkId   = -1;
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
  
  public void merge(MessageTrackingChunkStat otherChunk) {
    if(fromChunkId < 0) fromChunkId = otherChunk.getChunkId();
    toChunkId = otherChunk.getChunkId();
    trackingLostCount += otherChunk.getTrackingLostCount();
    trackingDuplicatedCount += otherChunk.getTrackingDuplicatedCount();
    Map<String, MessageTrackingLogChunkStat> otherLogStats = otherChunk.getLogStats();
    for(String logName : otherLogStats.keySet()) {
      MessageTrackingLogChunkStat logStat = logStats.get(logName);
      if(logStat == null) {
        logStat = new MessageTrackingLogChunkStat();
        logStats.put(logName, logStat);
      }
      logStat.merge(otherLogStats.get(logName));
    }
  }
  
  public void merge(AggregateMessageTrackingChunkStat other) {
    toChunkId = other.getToChunkId();
    trackingLostCount += other.getTrackingLostCount();
    trackingDuplicatedCount += other.getTrackingDuplicatedCount();
  }
}