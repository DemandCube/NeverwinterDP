package com.neverwinterdp.scribengin.dataflow;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.message.MessageTracking;
import com.neverwinterdp.message.MessageTrackingChunkStat;
import com.neverwinterdp.message.MessageTrackingRegistry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;

@Singleton
public class MTService {
  private int                        trackingWindowSize;
  private AtomicInteger              windowIdTracker ;
  private int                        currentChunkId ;
  private MessageTrackingRegistry    trackingRegistry;
  
  private ConcurrentHashMap<Integer, MessageTrackingChunkStat> outputChunkStats = new ConcurrentHashMap<>();
  private ConcurrentHashMap<Integer, MessageTrackingChunkStat> inputChunkStats  = new ConcurrentHashMap<>();

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
      currentChunkId = trackingRegistry.nextMessageChunkId();
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
  
  void log(ConcurrentHashMap<Integer, MessageTrackingChunkStat> holder, String name, MessageTracking mTracking) throws RegistryException {
    synchronized(holder) {
      MessageTrackingChunkStat chunk = holder.get(mTracking.getChunkId());
      if(chunk == null) {
        chunk = new  MessageTrackingChunkStat(name, mTracking.getChunkId(), trackingWindowSize);
        holder.put(chunk.getChunkId(), chunk);
      }
      chunk.log(mTracking);
    }
  }
  
  MessageTrackingChunkStat[] takeAll(ConcurrentHashMap<Integer, MessageTrackingChunkStat> map) {
    synchronized(map) {
      if(map.size() == 0) return null;
      MessageTrackingChunkStat[] array = new MessageTrackingChunkStat[map.size()];
      map.values().toArray(array);
      map.clear();
      return array;
    }
  }
  
  public void flushInput() throws RegistryException {
    MessageTrackingChunkStat[] array = takeAll(inputChunkStats);
    if(array == null) return;
    for(MessageTrackingChunkStat sel : array) trackingRegistry.saveProgress(sel);
  }
  
  public void flushOutput() throws RegistryException {
    MessageTrackingChunkStat[] array = takeAll(outputChunkStats);
    if(array == null) return;
    for(MessageTrackingChunkStat sel : array) trackingRegistry.saveProgress(sel);
  }
}