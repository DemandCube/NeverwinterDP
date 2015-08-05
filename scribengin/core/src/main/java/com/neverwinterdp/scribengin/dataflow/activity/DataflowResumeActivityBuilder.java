package com.neverwinterdp.scribengin.dataflow.activity;

import java.util.ArrayList;
import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.activity.ActivityBuilder;
import com.neverwinterdp.registry.activity.ActivityExecutionContext;
import com.neverwinterdp.registry.activity.ActivityStep;
import com.neverwinterdp.registry.activity.ActivityStepBuilder;
import com.neverwinterdp.registry.activity.ActivityStepExecutor;
import com.neverwinterdp.registry.txevent.TXEvent;
import com.neverwinterdp.registry.txevent.TXEventBroadcaster;
import com.neverwinterdp.registry.txevent.TXEventNotificationCompleteListener;
import com.neverwinterdp.registry.txevent.TXEventNotificationWatcher;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.event.DataflowEvent;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.service.DataflowService;

public class DataflowResumeActivityBuilder extends ActivityBuilder {
  public Activity build() {
    Activity activity = new Activity();
    activity.setDescription("Pause Dataflow Activity");
    activity.setType("resume-dataflow");
    activity.withCoordinator(DataflowActivityCoordinator.class);
    activity.withActivityStepBuilder(DataflowResumeActivityStepBuilder.class) ;
    return activity;
  }
  
  @Singleton
  static public class DataflowResumeActivityStepBuilder implements ActivityStepBuilder {
    
    @Override
    public List<ActivityStep> build(Activity activity, Injector container) throws Exception {
      List<ActivityStep> steps = new ArrayList<>() ;
      steps.add(new ActivityStep().
          withType("broadcast-resume-dataflow-worker").
          withExecutor(BroadcastResumeWorkerStepExecutor.class));
      
      steps.add(new ActivityStep().
          withType("set-dataflow-run-status").
          withExecutor(SetRunningDataflowStatusStepExecutor.class));
      return steps;
    }
  }
  
  @Singleton
  static public class BroadcastResumeWorkerStepExecutor implements ActivityStepExecutor {
    @Inject
    private DataflowService service ;
    
    @Override
    public void execute(ActivityExecutionContext ctx, Activity activity, ActivityStep step) throws Exception {
      DataflowRegistry dflRegistry = service.getDataflowRegistry();
      List<String> activeWorkers = dflRegistry.getWorkerRegistry().getActiveWorkerIds();
      TXEvent pEvent = new TXEvent("resume", DataflowEvent.RESUME);
      TXEventBroadcaster broadcaster = dflRegistry.getWorkerRegistry().getWorkerEventBroadcaster();
      TXEventNotificationWatcher watcher = 
          broadcaster.broadcast(pEvent, new TXEventNotificationCompleteListener());
      System.err.println("DataflowResumeActivity: broadcast resume for worker, workers = " + activeWorkers) ;
      int count = watcher.waitForNotifications(activeWorkers.size(), 30 * 1000);
      watcher.complete();
      if(count != activeWorkers.size()) {
        throw new Exception("Expect " + activeWorkers.size() + ", but get only " + count + " notification") ;
      }
      System.err.println("DataflowResumeActivity: broadcast successfully");
    }
  }
  
  @Singleton
  static public class SetRunningDataflowStatusStepExecutor implements ActivityStepExecutor {
    @Inject
    private DataflowService service ;
    
    @Override
    public void execute(ActivityExecutionContext ctx, Activity activity, ActivityStep step) throws Exception {
      DataflowRegistry dflRegistry = service.getDataflowRegistry();
      dflRegistry.setStatus(DataflowLifecycleStatus.RUNNING);
    }
  }
}