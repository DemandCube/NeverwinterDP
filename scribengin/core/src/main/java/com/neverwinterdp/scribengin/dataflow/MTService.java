package com.neverwinterdp.scribengin.dataflow;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.neverwinterdp.message.MessageTracking;
import com.neverwinterdp.message.MessageTrackingRegistry;
import com.neverwinterdp.message.WindowMessageTrackingStat;
import com.neverwinterdp.registry.ErrorCode;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;

public class MTService {
  private String                     name;
  private int                        trackingWindowSize;
  private int                        slidingWindowSize;
  private AtomicInteger              windowTrackingIdTracker ;
  private int                        currentWindowId = -1;
  private MessageTrackingRegistry    trackingRegistry;
  
  private ConcurrentHashMap<Integer, WindowMessageTrackingStat> windowStats = new ConcurrentHashMap<>();

  public MTService(String name, DataflowRegistry dflRegistry) throws RegistryException {
    this.name = name;
    DataflowDescriptor dflDescriptor = dflRegistry.getConfigRegistry().getDataflowDescriptor();
    this.trackingRegistry   = dflRegistry.getMessageTrackingRegistry();
    this.trackingWindowSize = dflDescriptor.getTrackingWindowSize();
    this.slidingWindowSize = dflDescriptor.getSlidingWindowSize();
    windowTrackingIdTracker = new AtomicInteger(0);
  }
  
  int nextWindowId(long maxWaitForDataRead) throws RegistryException, InterruptedException {
    int windowId = trackingRegistry.nextWindowId("output", slidingWindowSize);
    if(windowId > -1) return windowId;
    
    long stopTime = System.currentTimeMillis() + maxWaitForDataRead;
    while(System.currentTimeMillis() < stopTime) {
      windowId = trackingRegistry.nextWindowId("output", slidingWindowSize);
      if(windowId > -1) return windowId;
      Thread.sleep(500);
    }
    return windowId;
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
    return new MessageTracking(currentWindowId, windowTrackingId);
  }

  public void log(MessageTracking mTracking) throws RegistryException {
    WindowMessageTrackingStat windowStat = windowStats.get(mTracking.getWindowId());
    if(windowStat == null) {
      windowStat = new  WindowMessageTrackingStat(name, mTracking.getWindowId(), trackingWindowSize);
      windowStats.put(windowStat.getWindowId(), windowStat);
    }
    windowStat.log(mTracking);
  }
  
  WindowMessageTrackingStat[] takeAll() {
    if(windowStats.size() == 0) return null;
    WindowMessageTrackingStat[] array = new WindowMessageTrackingStat[windowStats.size()];
    windowStats.values().toArray(array);
    windowStats.clear();
    return array;
  }
  
  public void flush() throws RegistryException {
    WindowMessageTrackingStat[] array = takeAll();
    if(array == null) return;
    for(WindowMessageTrackingStat sel : array) trackingRegistry.saveProgress(sel);
    System.err.println("MTService flush(), name = " + name + ", num of window = " + array.length);
  }
}