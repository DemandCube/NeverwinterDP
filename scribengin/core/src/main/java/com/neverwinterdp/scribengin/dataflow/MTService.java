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
  private int                     trackingWindowSize;
  private AtomicInteger           windowIdTracker ;
  private int                     currentChunkId ;
  private MessageTrackingRegistry trackingRegistry;
  private ConcurrentHashMap<Integer, MessageTrackingChunkStat> chunkStats = new ConcurrentHashMap<>();

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

  synchronized public void log(MessageTracking mTracking) throws RegistryException {
    MessageTrackingChunkStat chunk = chunkStats.get(mTracking.getChunkId());
    if(chunk == null) {
      chunk = new  MessageTrackingChunkStat("output", mTracking.getChunkId(), trackingWindowSize);
      chunkStats.put(chunk.getChunkId(), chunk);
    }
    chunk.log(mTracking);
  }
  
  synchronized MessageTrackingChunkStat[] takeAll() {
    if(chunkStats.size() == 0) return null;
    MessageTrackingChunkStat[] array = new MessageTrackingChunkStat[chunkStats.size()];
    chunkStats.values().toArray(array);
    chunkStats.clear();
    return array;
  }
  
  public void flush() throws RegistryException {
    MessageTrackingChunkStat[] array = takeAll();
    if(array == null) return;
    for(MessageTrackingChunkStat sel : array) {
      trackingRegistry.saveProgress(sel);
    }
  }
}