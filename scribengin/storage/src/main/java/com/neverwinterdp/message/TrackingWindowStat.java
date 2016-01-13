package com.neverwinterdp.message;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.DataFormatException;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.neverwinterdp.message.MessageTracking;
import com.neverwinterdp.message.MessageTrackingLog;
import com.neverwinterdp.util.io.IOUtil;
import com.neverwinterdp.util.text.TabularFormater;

public class TrackingWindowStat {
  static DecimalFormat ID_FORMAT = new DecimalFormat("00000000");
  
  private String                             name;
  private int                                windowId;
  private int                                maxWindowSize;
  private int                                windowSize;
  private int                                trackingProgress;
  private int                                trackingNoLostTo;
  private int                                trackingLostCount;
  private int                                trackingDuplicatedCount;
  private int                                trackingCount;
  private Map<String, TrackingWindowLogStat> logStats;
  
  private boolean complete = false;

  transient private boolean persisted = false;
  transient private BitSet  bitSet;

  public TrackingWindowStat() {}
  
  public TrackingWindowStat(String name, int windowId, int maxWindowSize) {
    this.name      = name ;
    this.windowId  =  windowId;
    this.maxWindowSize = maxWindowSize;
    this.windowSize = maxWindowSize;
    this.logStats  = new HashMap<>();
    this.bitSet    = new BitSet(maxWindowSize);
  }

  public String getName() { return name; }
  public void setName(String name) { this.name = name; }

  public int getWindowId() { return windowId; }
  public void setWindowId(int windowId) { this.windowId = windowId; }

  public int getWindowSize() { return windowSize; }
  public void setWindowSize(int windowSize) { this.windowSize = windowSize; }

  public int getMaxWindowSize() { return maxWindowSize; }
  public void setMaxWindowSize(int size) { this.maxWindowSize = size;}

  public int getTrackingProgress() { return trackingProgress; }
  public void setTrackingProgress(int trackingProgress) {
    this.trackingProgress = trackingProgress;
  }

  public int getTrackingNoLostTo() { return trackingNoLostTo; }
  public void setTrackingNoLostTo(int trackingNoLostTo) {
    this.trackingNoLostTo = trackingNoLostTo;
  }

  public int getTrackingLostCount() { return trackingLostCount; }
  public void setTrackingLostCount(int trackingLostCount) {
    this.trackingLostCount = trackingLostCount;
  }

  public int getTrackingDuplicatedCount() { return trackingDuplicatedCount; }
  public void setTrackingDuplicatedCount(int trackingDuplicatedCount) {
    this.trackingDuplicatedCount = trackingDuplicatedCount;
  }

  public int getTrackingCount() { return trackingCount; }
  public void setTrackingCount(int trackingCount) {
    this.trackingCount = trackingCount;
  }

  public Map<String, TrackingWindowLogStat> getLogStats() { return logStats; }
  public void setLogStats(Map<String, TrackingWindowLogStat> logStats) { this.logStats = logStats; }

  public boolean isComplete() { return complete;}
  public void setComplete(boolean complete) {
    this.complete = complete;
  }
  
  public byte[] getBitSetData() throws IOException {
    return IOUtil.compress(bitSet.toByteArray()) ;
  }
  
  public void setBitSetData(byte[] data) throws IOException, DataFormatException {
    byte[] bitData = IOUtil.decompress(data);
    bitSet = BitSet.valueOf(bitData);
  }
  
  @JsonIgnore
  public boolean isPersisted() { return this.persisted; }
  public void    setPersisted(boolean b) { persisted = b; }
  
  synchronized public int log(MessageTracking mTracking) {
    if(mTracking.getWindowId() != windowId) {
      throw new RuntimeException("The chunk id is not matched, chunkId = " + windowId + ", message chunk id = " + mTracking.getWindowId());
    }
    int idx = mTracking.getTrackingId();
    if(idx > maxWindowSize) {
      throw new RuntimeException("The tracking id is greater than the chunk size" + maxWindowSize);
    }
    
    if(idx > trackingProgress) trackingProgress = idx;
    if(bitSet.get(idx)) {
      trackingDuplicatedCount++;
    } else {
      bitSet.set(idx, true); 
      trackingCount++;
      List<MessageTrackingLog> logs = mTracking.getLogs();
      if(logs != null) {
        for(int i = 0; i < logs.size(); i++) {
          MessageTrackingLog log = logs.get(i);
          TrackingWindowLogStat logStat = logStats.get(log.getName());
          if(logStat == null) {
            logStat = new TrackingWindowLogStat();
            logStats.put(log.getName(), logStat);
          }
          logStat.log(mTracking, log);
        }
      }
    }
    return trackingCount;
  }
  
  synchronized public void update() {
    trackingLostCount = 0;
    trackingNoLostTo  = -1;
    for(int i = 0; i <= trackingProgress; i++) {
      if(!bitSet.get(i)) {
        if(trackingNoLostTo < 0) trackingNoLostTo = i;
        trackingLostCount++ ;
      }
    }
    if(trackingNoLostTo < 0) trackingNoLostTo = trackingProgress;
    if(trackingNoLostTo + 1 == windowSize) complete = true;
  }
  
  synchronized public void merge(TrackingWindowStat other) {
    if(other.trackingProgress > trackingProgress) trackingProgress = other.trackingProgress;
    trackingDuplicatedCount += other.trackingDuplicatedCount;
    
    trackingNoLostTo  = -1;
    trackingLostCount = 0;
    for(int idx = 0; idx <= trackingProgress; idx++) {
      if(other.bitSet.get(idx)) {
        if(bitSet.get(idx)) {
          trackingDuplicatedCount++;
        } else {
          trackingCount++;
          bitSet.set(idx, true) ;
        }
      }
      
      if(!bitSet.get(idx)) {
        if(trackingNoLostTo < 0) trackingNoLostTo = idx;
        trackingLostCount++ ;
      }
    }
    if(trackingNoLostTo < 0) trackingNoLostTo = trackingProgress;
    if(trackingNoLostTo + 1 == windowSize) complete = true;
    
    for(String otherLogKey : other.logStats.keySet()) {
      TrackingWindowLogStat logStat = logStats.get(otherLogKey);
      if(logStat == null) {
        logStat = new TrackingWindowLogStat();
        logStats.put(otherLogKey, logStat);
      }
      logStat.merge(other.logStats.get(otherLogKey));
    }
  }
  
  public String toWindowIdName() { return toWindowIdName(windowId); }
  
  public String toFormattedText() {
    return toFormattedText(this);
  }
  
  static public String toFormattedText(TrackingWindowStat ... window) {
    TabularFormater ft = 
      new TabularFormater("Name", "Window Id", "Type", "Window Size", "Count", "Progress", "No Lost To", "Duplicated");
    for(int i = 0; i < window.length; i++) {
      window[i].update();
      ft.addRow(
        window[i].getName(), window[i].getWindowId(), "Tracking", window[i].getWindowSize(), 
        window[i].getTrackingCount(), window[i].getTrackingProgress(), window[i].getTrackingNoLostTo(), window[i].getTrackingDuplicatedCount()
      );
      for(String logKey : window[i].logStats.keySet()) {
        TrackingWindowLogStat logStat = window[i].logStats.get(logKey);
        ft.addRow("", "", logKey, "", logStat.getCount(), "", "", "");
      }
    }
    return ft.getFormattedText() ;
  }
  
  final static public String toWindowIdName(int chunkId) {
    return "window-" + ID_FORMAT.format(chunkId);
  }
}
