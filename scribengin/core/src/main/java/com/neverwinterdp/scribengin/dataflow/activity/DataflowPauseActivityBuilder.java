package com.neverwinterdp.scribengin.dataflow.activity;

import java.util.ArrayList;
import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.activity.ActivityBuilder;
import com.neverwinterdp.registry.activity.ActivityExecutionContext;
import com.neverwinterdp.registry.activity.ActivityStep;
import com.neverwinterdp.registry.activity.ActivityStepBuilder;
import com.neverwinterdp.registry.activity.ActivityStepExecutor;
import com.neverwinterdp.registry.event.WaitingNodeEventListener;
import com.neverwinterdp.registry.event.WaitingRandomNodeEventListener;
import com.neverwinterdp.registry.txevent.TXEvent;
import com.neverwinterdp.registry.txevent.TXEventNotificationCompleteListener;
import com.neverwinterdp.registry.txevent.TXEventNotificationWatcher;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.event.DataflowEvent;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowWorkerRegistry;
import com.neverwinterdp.scribengin.dataflow.service.DataflowService;
import com.neverwinterdp.scribengin.dataflow.worker.DataflowWorkerStatus;

public class DataflowPauseActivityBuilder extends ActivityBuilder {
  public Activity build() {
    Activity activity = new Activity() ;
    activity.setDescription("Pause Dataflow Activity");
    activity.setType("pause-dataflow");
    activity.withCoordinator(DataflowActivityCoordinator.class);
    activity.withActivityStepBuilder(DataflowPauseActivityStepBuilder.class);
    return activity;
  }
  
  @Singleton
  static public class DataflowPauseActivityStepBuilder implements ActivityStepBuilder {
    
    @Override
    public List<ActivityStep> build(Activity activity, Injector container) throws Exception {
      List<ActivityStep> steps = new ArrayList<>() ;
      
      steps.add(new ActivityStep().
          withType("broadcast-pause-dataflow-worker").
          withExecutor(BroadcastPauseWorkerStepExecutor.class));
      
      steps.add(new ActivityStep().
          withType("set-dataflow-pause-status").
          withExecutor(SetPauseDataflowStatusStepExecutor.class));
      return steps;
    }
  }
  
  @Singleton
  static public class BroadcastPauseWorkerStepExecutor implements ActivityStepExecutor {
    @Inject
    private DataflowService service ;
    
    @Override
    public void execute(ActivityExecutionContext ctx, Activity activity, ActivityStep step) throws Exception {
      DataflowRegistry dflRegistry = service.getDataflowRegistry();
      if(DataflowLifecycleStatus.RUNNING != dflRegistry.getStatus()) {
        ctx.setAbort(true);
        return ;
      }
      List<String> activeWorkers = dflRegistry.getWorkerRegistry().getActiveWorkerIds();
      TXEvent pEvent = new TXEvent("pause", DataflowEvent.PAUSE);
      DataflowWorkerRegistry workerRegistry = dflRegistry.getWorkerRegistry();
      TXEventNotificationWatcher watcher = workerRegistry.getWorkerEventBroadcaster().broadcast(pEvent);
      int count = watcher.waitForNotifications(activeWorkers.size(), 60 * 1000);
      if(count != activeWorkers.size()) {
        throw new Exception("Expect " + activeWorkers.size() + ", but get only " + count + " notification") ;
      }
      watcher.complete();
    }
  }
  
  @Singleton
  static public class SetPauseDataflowStatusStepExecutor implements ActivityStepExecutor {
    @Inject
    private DataflowService service ;
    
    @Override
    public void execute(ActivityExecutionContext ctx, Activity activity, ActivityStep step) throws Exception {
      DataflowRegistry dflRegistry = service.getDataflowRegistry();
      dflRegistry.setStatus(DataflowLifecycleStatus.PAUSE);
    }
  }
}
