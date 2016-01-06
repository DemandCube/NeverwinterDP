package com.neverwinterdp.message;

import java.util.ArrayList;
import java.util.List;

public class MessageTracking {
  private long                     timestamp ;
  private int                      windowId;
  private int                      trackingId;
  private List<MessageTrackingLog> logs;

  public MessageTracking() {}
  
  public MessageTracking(int windowId, int trackingId) {
    this.timestamp  = System.currentTimeMillis();
    this.windowId    = windowId;
    this.trackingId = trackingId;
    this.logs       = new ArrayList<>();
  }
  
  public long getTimestamp() { return timestamp; }
  public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

  public int  getWindowId() { return windowId; }
  public void setWindowId(int chunkId) { this.windowId = chunkId; }

  public int  getTrackingId() { return trackingId; }
  public void setTrackingId(int trackingId) { this.trackingId = trackingId; }

  public List<MessageTrackingLog> getLogs() { return logs; }
  public void setLogs(List<MessageTrackingLog> logs) {
    this.logs = logs;
  }
  
  public void add(MessageTrackingLog tag) {
    logs.add(tag);
  }
}
