package com.neverwinterdp.scribengin.dataflow;

import java.util.concurrent.atomic.AtomicInteger;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.message.MessageTracking;
import com.neverwinterdp.message.MessageTrackingRegistry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;

@Singleton
public class MTService {
  private int                     trackingWindowSize;
  private AtomicInteger           windowIdTracker ;
  private int                     currentChunkId ;
  private MessageTrackingRegistry trackingRegistry;

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

  public void log(MessageTracking messageTracking) throws RegistryException {
    messageTracking.getChunkId();
  }
}
