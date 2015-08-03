package com.neverwinterdp.scribengin.dataflow.activity;

import java.util.ArrayList;
import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.activity.ActivityBuilder;
import com.neverwinterdp.registry.activity.ActivityExecutionContext;
import com.neverwinterdp.registry.activity.ActivityStep;
import com.neverwinterdp.registry.activity.ActivityStepBuilder;
import com.neverwinterdp.registry.activity.ActivityStepExecutor;
import com.neverwinterdp.registry.event.NodeChildrenWatcher;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.txevent.TXEvent;
import com.neverwinterdp.registry.txevent.TXEventNotificationCompleteListener;
import com.neverwinterdp.registry.txevent.TXEventNotificationWatcher;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.event.DataflowEvent;
import com.neverwinterdp.scribengin.dataflow.service.DataflowService;

public class DataflowStopActivityBuilder extends ActivityBuilder {
  public Activity build() {
    Activity activity = new Activity();
    activity.setDescription("Stop Dataflow Activity");
    activity.setType("stop-dataflow");
    activity.withCoordinator(DataflowActivityCoordinator.class);
    activity.withActivityStepBuilder(DataflowStopActivityStepBuilder.class);
    return activity;
  }
  
  @Singleton
  static public class DataflowStopActivityStepBuilder implements ActivityStepBuilder {
    @Override
    public List<ActivityStep> build(Activity activity, Injector container) throws Exception {
      List<ActivityStep> steps = new ArrayList<>() ;
      steps.add(new ActivityStep().
          withType("check-dataflow-status").
          withExecutor(CheckDataflowStatusStepExecutor.class));
      steps.add(new ActivityStep().
          withType("broadcast-stop-dataflow-worker").
          withExecutor(BroadcastStopWorkerStepExecutor.class));
      
      steps.add(new ActivityStep().
          withType("set-dataflow-stop-status").
          withExecutor(SetStopDataflowStatusStepExecutor.class));
      return steps;
    }
  }

  @Singleton
  static public class CheckDataflowStatusStepExecutor implements ActivityStepExecutor {
    @Inject
    private DataflowRegistry dflRegistry ;
    
    @Override
    public void execute(ActivityExecutionContext ctx, Activity activity, ActivityStep step) throws Exception {
      if(DataflowLifecycleStatus.RUNNING != dflRegistry.getStatus()) {
       ctx.setAbort(true);
      }
    }
  }
  
  
  @Singleton
  static public class BroadcastStopWorkerStepExecutor implements ActivityStepExecutor {
    @Inject
    private DataflowService service ;
    
    @Override
    public void execute(ActivityExecutionContext ctx, Activity activity, ActivityStep step) throws Exception {
      DataflowRegistry dflRegistry = service.getDataflowRegistry();
      List<String> workers = dflRegistry.getActiveWorkerNames() ;
      TXEvent pEvent = new TXEvent("stop", DataflowEvent.STOP);
      TXEventNotificationWatcher watcher = 
          dflRegistry.getWorkerEventBroadcaster().broadcast(pEvent, new TXEventNotificationCompleteListener());
      watcher.waitForNotifications(workers.size(), 45 * 1000);
    }
  }
  
  @Singleton
  static public class SetStopDataflowStatusStepExecutor implements ActivityStepExecutor {
    @Inject
    private DataflowService service ;
    
    @Override
    public void execute(ActivityExecutionContext ctx, Activity activity, ActivityStep step) throws Exception {
      DataflowRegistry dflRegistry = service.getDataflowRegistry();
      dflRegistry.setStatus(DataflowLifecycleStatus.STOP);
    }
  }
}
