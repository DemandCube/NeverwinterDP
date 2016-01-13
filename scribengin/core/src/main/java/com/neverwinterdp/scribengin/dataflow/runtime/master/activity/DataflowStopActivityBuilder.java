package com.neverwinterdp.scribengin.dataflow.runtime.master.activity;

import java.util.ArrayList;
import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.neverwinterdp.message.TrackingWindowRegistry;
import com.neverwinterdp.message.TrackingWindowReport;
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
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.runtime.master.MasterService;
import com.neverwinterdp.scribengin.dataflow.runtime.worker.DataflowWorkerEvent;

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
          withType("broadcast-stop-input-dataflow-worker").
          withExecutor(BroadcastStopInputStepExecutor.class));
      
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
  static public class BroadcastStopWorkerStepExecutor implements ActivityStepExecutor {
    @Inject
    private MasterService service ;
    
    @Override
    public void execute(ActivityExecutionContext ctx, Activity activity, ActivityStep step) throws Exception {
      DataflowRegistry dflRegistry = service.getDataflowRegistry();
      if(DataflowLifecycleStatus.RUNNING != dflRegistry.getDataflowStatus()) {
        ctx.setAbort(true);
        return ;
      }
      
      List<String> workers = dflRegistry.getWorkerRegistry().getActiveWorkerIds() ;
      TXEvent pEvent = new TXEvent("stop", DataflowWorkerEvent.StopWorker);
      TXEventBroadcaster broadcaster = dflRegistry.getWorkerRegistry().getWorkerEventBroadcaster();
      TXEventNotificationWatcher watcher = broadcaster.broadcast(pEvent, new TXEventNotificationCompleteListener());
      int countNotification = watcher.waitForNotifications(workers.size(), 60 * 1000);
      if(countNotification != workers.size()) {
        throw new Exception("Expect " + workers.size() + ", but only get " + countNotification) ;
      }
      watcher.complete();
    }
  }
  
  @Singleton
  static public class BroadcastStopInputStepExecutor implements ActivityStepExecutor {
    @Inject
    private MasterService service ;
    
    @Override
    public void execute(ActivityExecutionContext ctx, Activity activity, ActivityStep step) throws Exception {
      DataflowRegistry dflRegistry = service.getDataflowRegistry();
      if(DataflowLifecycleStatus.RUNNING != dflRegistry.getDataflowStatus()) {
        ctx.setAbort(true);
        return ;
      }
      
      
      System.err.println("BroadcastStopInputStepExecutor: start broadcast StopInput");
      List<String> workers = dflRegistry.getWorkerRegistry().getActiveWorkerIds() ;
      TXEvent pEvent = new TXEvent("stop-input", DataflowWorkerEvent.StopInput);
      TXEventBroadcaster broadcaster = dflRegistry.getWorkerRegistry().getWorkerEventBroadcaster();
      TXEventNotificationWatcher watcher = broadcaster.broadcast(pEvent, new TXEventNotificationCompleteListener());
      int countNotification = watcher.waitForNotifications(workers.size(), 60 * 1000);
      if(countNotification != workers.size()) {
        throw new Exception("Expect " + workers.size() + ", but only get " + countNotification) ;
      }
      watcher.complete();
      System.err.println("BroadcastStopInputStepExecutor: complete broadcast StopInput, num of worker = " + workers.size());
      
      TrackingWindowRegistry mtRegistry = dflRegistry.getMessageTrackingRegistry();
      int checkCount = 0;
      boolean noMessageLeft = false;
      while(checkCount < 120 && !noMessageLeft) {
        Thread.sleep(500);
        TrackingWindowReport trackingReport  = mtRegistry.getReport();
        int commitWindowLefts = mtRegistry.getProgressCommitWindows().size();
        System.err.println("BroadcastStopInputStepExecutor: tracking count = " + trackingReport.getTrackingCount() + ", commitWindowLefts = " + commitWindowLefts);
        if(commitWindowLefts == 0) noMessageLeft = true;
        checkCount++;
      }
      if(!noMessageLeft) {
        throw new Exception("Expect no message in the scribengin") ;
      }
    }
  }
  
  @Singleton
  static public class SetStopDataflowStatusStepExecutor implements ActivityStepExecutor {
    @Inject
    private MasterService service ;
    
    @Override
    public void execute(ActivityExecutionContext ctx, Activity activity, ActivityStep step) throws Exception {
      DataflowRegistry dflRegistry = service.getDataflowRegistry();
      dflRegistry.setDataflowStatus(DataflowLifecycleStatus.STOP);
      System.err.println("DataflowService Stop Activity set STOP status done!!!") ;
    }
  }
}
