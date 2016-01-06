package com.neverwinterdp.message;

import java.util.HashMap;
import java.util.Map;

public class WindowId {
  private Map<String, Tracker> trackers = new HashMap<String, Tracker>();
  
  public Tracker getTracker(String name, boolean create) {
    Tracker tracker = trackers.get(name);
    if(tracker == null && create) {
      tracker = new Tracker(name);
      trackers.put(name, tracker);
    }
    return tracker;
  }
  
  public Map<String, Tracker> getTrackers() { return trackers; }
  public void setTrackers(Map<String, Tracker> trackers) { this.trackers = trackers; }

  static public class Tracker {
    private String name;
    private int    completeWindowCount = 0;

    public Tracker() { }
    
    public Tracker(String name) {
      this.name = name;
    }
    
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    
    public int getCompleteWindowCount() { return completeWindowCount; }
    public void setCompleteWindowCount(int completeWindowCount) {
      this.completeWindowCount = completeWindowCount;
    }
  }
}