package com.neverwinterdp.scribengin.dataflow;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.neverwinterdp.message.MessageTracking;
import com.neverwinterdp.message.TrackingWindow;
import com.neverwinterdp.message.TrackingWindowRegistry;
import com.neverwinterdp.message.TrackingWindowStat;
import com.neverwinterdp.registry.ErrorCode;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;

public class MTService {
  private String                 name;
  private int                    trackingWindowSize;
  private AtomicInteger          windowTrackingIdTracker;
  private int                    currentWindowId = -1;
  private TrackingWindowRegistry trackingRegistry;
  
  private ConcurrentHashMap<Integer, TrackingWindow> windows = new ConcurrentHashMap<>();
  
  private ConcurrentHashMap<Integer, TrackingWindowStat> windowStats = new ConcurrentHashMap<>();

  public MTService(String name, DataflowRegistry dflRegistry) throws RegistryException {
    this.name = name;
    DataflowDescriptor dflDescriptor = dflRegistry.getConfigRegistry().getDataflowDescriptor();
    this.trackingRegistry   = dflRegistry.getMessageTrackingRegistry();
    this.trackingWindowSize = dflDescriptor.getTrackingWindowSize();
    windowTrackingIdTracker = new AtomicInteger(0);
  }
  
  int nextWindowId(long maxWaitForDataRead) throws RegistryException, InterruptedException {
    return trackingRegistry.nextWindowId();
  }
  
  public boolean hasNextMessageTracking(long maxWaitForDataRead) throws RegistryException, InterruptedException {
    if(currentWindowId == -1) {
      currentWindowId = nextWindowId(maxWaitForDataRead);
      if(currentWindowId == -1) return false;
      windowTrackingIdTracker.set(0);
    }
    
    if(windowTrackingIdTracker.get() < trackingWindowSize) {
      return true;
    } else {
      currentWindowId = nextWindowId(maxWaitForDataRead);
      if(currentWindowId == -1) return false;
      windowTrackingIdTracker.set(0);
    }
    return true;
  }
  
  public MessageTracking nextMessageTracking() throws RegistryException {
    if(windowTrackingIdTracker.get() >= trackingWindowSize || currentWindowId == -1) {
      throw new RegistryException(ErrorCode.Unknown, "the tracking window id or the current window id is not in the valid state");
    }
    int windowTrackingId = windowTrackingIdTracker.getAndIncrement();
    TrackingWindow window = windows.get(currentWindowId);
    if(window == null) {
      window = new TrackingWindow(this.currentWindowId, trackingWindowSize);
      windows.put(currentWindowId, window);
    }
    window.setWindowSize(windowTrackingId + 1);
    return new MessageTracking(currentWindowId, windowTrackingId);
  }

  public void log(MessageTracking mTracking) throws RegistryException {
    TrackingWindowStat windowStat = windowStats.get(mTracking.getWindowId());
    if(windowStat == null) {
      windowStat = new  TrackingWindowStat(name, mTracking.getWindowId(), trackingWindowSize);
      windowStats.put(windowStat.getWindowId(), windowStat);
    }
    windowStat.log(mTracking);
  }
  
  public void flushWindows() throws RegistryException {
    if(windows.size() == 0) return ;
    TrackingWindow[] array = new TrackingWindow[windows.size()];
    windows.values().toArray(array);
    windows.clear();
    trackingRegistry.saveWindow(array);
    currentWindowId = -1;
  }
  
  public void flushWindowStats() throws RegistryException {
    if(windowStats.size() == 0) return ;
    TrackingWindowStat[] array = new TrackingWindowStat[windowStats.size()];
    windowStats.values().toArray(array);
    windowStats.clear();
    trackingRegistry.saveProgress(array);
  }
}