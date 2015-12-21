package com.neverwinterdp.message;

import java.util.ArrayList;
import java.util.List;

public class MessageTracking {
  private int                      chunkId;
  private int                      trackingId;
  private List<MessageTrackingLog> logs;

  public MessageTracking() {}
  
  public MessageTracking(int chunkId, int trackingId) {
    this.chunkId    = chunkId;
    this.trackingId = trackingId;
    this.logs = new ArrayList<>();
  }
  
  public int getChunkId() { return chunkId; }
  public void setChunkId(int chunkId) { this.chunkId = chunkId; }

  public int getTrackingId() { return trackingId; }
  public void setTrackingId(int trackingId) { this.trackingId = trackingId; }

  public List<MessageTrackingLog> getLogs() { return logs; }
  public void setLogs(List<MessageTrackingLog> trackingLogs) {
    this.logs = trackingLogs;
  }
  
  public void add(MessageTrackingLog log) {
    logs.add(log);
  }
}
