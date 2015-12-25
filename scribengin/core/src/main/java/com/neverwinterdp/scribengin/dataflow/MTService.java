package com.neverwinterdp.scribengin.dataflow;

import java.util.Iterator;
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
  static AtomicInteger counter = new AtomicInteger();
  
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
    counter.incrementAndGet();
  }
  
  public void flush() throws RegistryException {
    if(chunkStats.size() == 0) return;
    Iterator<Integer> i = chunkStats.keySet().iterator();
    while(i.hasNext()) {
      Integer chunkId = i.next();
      MessageTrackingChunkStat chunkStat = chunkStats.remove(chunkId);
      trackingRegistry.saveProgress(chunkStat);
    }
  }
}