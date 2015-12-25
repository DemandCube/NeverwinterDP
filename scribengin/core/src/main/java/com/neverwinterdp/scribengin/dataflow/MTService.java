package com.neverwinterdp.scribengin.dataflow;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PreDestroy;

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
  private FlushThread flushThread;

  @Inject
  public void onInject(DataflowRegistry dflRegistry) throws RegistryException {
    DataflowDescriptor dflDescriptor = dflRegistry.getConfigRegistry().getDataflowDescriptor();
    this.trackingRegistry = dflRegistry.getMessageTrackingRegistry();
    this.trackingWindowSize = dflDescriptor.getTrackingWindowSize();
    windowIdTracker = new AtomicInteger(trackingWindowSize);
    flushThread = new FlushThread();
    flushThread.start();
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
  
  public void flush() throws RegistryException {
    Iterator<Integer> i = chunkStats.keySet().iterator();
    while(i.hasNext()) {
      Integer chunkId = i.next();
      MessageTrackingChunkStat chunkStat = chunkStats.remove(chunkId);
      trackingRegistry.saveProgress(chunkStat);
    }
  }
  
  @PreDestroy
  public void onDestroy() throws InterruptedException, RegistryException {
    if(flushThread != null) {
      boolean flushThreadTerminated = flushThread.terminate(30000);
      flushThread = null;
      if(flushThreadTerminated) flush();
    }
  }
  
  public class FlushThread extends Thread {
    private boolean interrupt = false;
    private boolean terminated = false;
    
    public void run() {
      while(!interrupt) {
        try {
          flush();
          Thread.sleep(10000);
        } catch (RegistryException e) {
          e.printStackTrace();
        } catch (InterruptedException e) {
        }
      }
      synchronized(this) {
        terminated = true;
        notifyAll();
      }
    }
    
    synchronized public boolean terminate(long maxWaitTime) throws InterruptedException {
      interrupt = true;
      if(!terminated) {
        wait(maxWaitTime);
      }
      return terminated;
    }
  }
}