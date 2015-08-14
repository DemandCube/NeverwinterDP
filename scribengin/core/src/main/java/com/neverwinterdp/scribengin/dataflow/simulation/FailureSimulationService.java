package com.neverwinterdp.scribengin.dataflow.simulation;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.activity.ActivityStep;
import com.neverwinterdp.registry.queue.DistributedQueue;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.service.DataflowService;
import com.neverwinterdp.scribengin.dataflow.simulation.FailureConfig.FailurePoint;

@Singleton
public class FailureSimulationService {
  @Inject
  private DataflowService  dflService ;
  
  @Inject
  private DataflowRegistry dflRegistry ;
  
  private FailureConfig    currentFailureConfig;
  
  synchronized public void runFailureSimulation(Activity activity, ActivityStep step, FailurePoint failurePoint) throws Exception {
    try {
    DistributedQueue failureQueue = dflRegistry.getMasterRegistry().getFailureEventQueue();
    if(currentFailureConfig == null) {
      currentFailureConfig = failureQueue.elementAs(FailureConfig.class);
    }
    if(currentFailureConfig == null) return;
    if(currentFailureConfig.matches(activity, step, failurePoint)) {
      System.err.println("runFailureSimulation matches activity = " + activity.getType() + ", step = " + step.getId()); 
      long delay = currentFailureConfig.getDelay();
      failureQueue.remove();
      currentFailureConfig = null ;
      if(delay <= 0) {
        dflService.simulateKill();
      } else {
        new Thread() {
          public void run() {
            try {
              Thread.sleep(currentFailureConfig.getDelay());
              dflService.simulateKill();
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        }.start();
      }
    }
    } catch(Throwable t) {
      t.printStackTrace();
    }
  }
}