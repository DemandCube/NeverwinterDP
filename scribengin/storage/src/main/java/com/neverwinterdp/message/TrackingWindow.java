package com.neverwinterdp.message;

import java.text.DecimalFormat;

public class TrackingWindow {
  static DecimalFormat ID_FORMAT = new DecimalFormat("00000000");
  
  private int windowId;
  private int maxWindowSize;
  private int windowSize;
  
  public TrackingWindow() {}
  
  public TrackingWindow(int windowId, int maxWindowSize) {
    this.windowId   = windowId;
    this.maxWindowSize = maxWindowSize;
  }
  
  public int getWindowId() { return windowId; }
  public void setWindowId(int windowId) { this.windowId = windowId; }
  
  public int getMaxWindowSize() { return maxWindowSize; }
  public void setMaxWindowSize(int maxWindowSize) { this.maxWindowSize = maxWindowSize; }
  
  public int getWindowSize() { return windowSize; }
  public void setWindowSize(int windowSize) { this.windowSize = windowSize; }
  
  public String toWindowIdName() { return toIdName(windowId); }
  
  final static public String toIdName(int windowId) {
    return "window-" + ID_FORMAT.format(windowId);
  }
}
