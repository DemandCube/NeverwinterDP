package com.neverwinterdp.scribengin.dataflow;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.message.MessageTracking;
import com.neverwinterdp.message.WindowMessageTrackingStat;
import com.neverwinterdp.message.MessageTrackingRegistry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;

@Singleton
public class MTService {
  private int                        trackingWindowSize;
  private AtomicInteger              windowIdTracker ;
  private int                        currentChunkId ;
  private MessageTrackingRegistry    trackingRegistry;
  
  private ConcurrentHashMap<Integer, WindowMessageTrackingStat> outputChunkStats = new ConcurrentHashMap<>();
  private ConcurrentHashMap<Integer, WindowMessageTrackingStat> inputChunkStats  = new ConcurrentHashMap<>();

  @Inject
  public void onInject(DataflowRegistry dflRegistry) throws RegistryException {
    DataflowDescriptor dflDescriptor = dflRegistry.getConfigRegistry().getDataflowDescriptor();
    this.trackingRegistry = dflRegistry.getMessageTrackingRegistry();
    this.trackingWindowSize = dflDescriptor.getTrackingWindowSize();
    windowIdTracker = new AtomicInteger(trackingWindowSize);
  }
  
  synchronized public MessageTracking nextMessageTracking() throws RegistryException {
    int currentWindowId = windowIdTracker.incrementAndGet();
    if(currentWindowId >= trackingWindowSize) {
      currentChunkId = trackingRegistry.nextWindowId();
      windowIdTracker.set(0);
      currentWindowId = 0;
    }
    return new MessageTracking(currentChunkId, currentWindowId);
  }

  public void logInput(MessageTracking mTracking) throws RegistryException {
    log(inputChunkStats, "input", mTracking);
  }
  
  public void logOutput(MessageTracking mTracking) throws RegistryException {
    log(outputChunkStats, "output", mTracking);
  }
  
  void log(ConcurrentHashMap<Integer, WindowMessageTrackingStat> holder, String name, MessageTracking mTracking) throws RegistryException {
    synchronized(holder) {
      WindowMessageTrackingStat chunk = holder.get(mTracking.getWindowId());
      if(chunk == null) {
        chunk = new  WindowMessageTrackingStat(name, mTracking.getWindowId(), trackingWindowSize);
        holder.put(chunk.getWindowId(), chunk);
      }
      chunk.log(mTracking);
    }
  }
  
  WindowMessageTrackingStat[] takeAll(ConcurrentHashMap<Integer, WindowMessageTrackingStat> map) {
    synchronized(map) {
      if(map.size() == 0) return null;
      WindowMessageTrackingStat[] array = new WindowMessageTrackingStat[map.size()];
      map.values().toArray(array);
      map.clear();
      return array;
    }
  }
  
  public void flushInput() throws RegistryException {
    WindowMessageTrackingStat[] array = takeAll(inputChunkStats);
    if(array == null) return;
    for(WindowMessageTrackingStat sel : array) trackingRegistry.saveProgress(sel);
  }
  
  public void flushOutput() throws RegistryException {
    WindowMessageTrackingStat[] array = takeAll(outputChunkStats);
    if(array == null) return;
    for(WindowMessageTrackingStat sel : array) trackingRegistry.saveProgress(sel);
  }
}