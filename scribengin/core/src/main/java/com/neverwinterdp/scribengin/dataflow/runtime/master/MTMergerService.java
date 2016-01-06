package com.neverwinterdp.scribengin.dataflow.runtime.master;

import com.neverwinterdp.message.MessageTrackingRegistry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;

public class MTMergerService {
  private MessageTrackingRegistry trackingRegistry;
  private FlushThread flushThread;
  
  public MTMergerService(DataflowRegistry dflRegistry) {
    trackingRegistry = dflRegistry.getMessageTrackingRegistry();
    flushThread = new FlushThread();
    flushThread.start();
  }
  
  public void onDestroy() throws InterruptedException, RegistryException {
    if(flushThread != null) {
      boolean flushThreadTerminated = flushThread.terminate(30000);
      if(flushThreadTerminated) {
        flush();
      }
    }
  }
  
  public void flush() throws RegistryException {
    trackingRegistry.mergeProgress("input");
    trackingRegistry.mergeFinishedReport("input");
    
    trackingRegistry.mergeProgress("output");
    trackingRegistry.mergeFinishedReport("output");
  }
  
  public class FlushThread extends Thread {
    boolean interrupted = false;
    boolean terminated = false;
    
    public void run() {
      while(!interrupted) {
        try {
          flush();
          Thread.sleep(15000);
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
    
    synchronized boolean terminate(long maxWaitTime) throws InterruptedException {
      interrupted = true;
      interrupt();
      if(!terminated) {
        wait(maxWaitTime);
      }
      return terminated;
    }
  }
}
