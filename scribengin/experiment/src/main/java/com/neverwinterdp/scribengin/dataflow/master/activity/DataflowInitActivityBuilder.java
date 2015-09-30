package com.neverwinterdp.scribengin.dataflow.master.activity;

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
import com.neverwinterdp.scribengin.dataflow.master.DataflowMasterService;

public class DataflowInitActivityBuilder extends ActivityBuilder {
  public Activity build() {
    Activity activity = new Activity();
    activity.setDescription("Init Dataflow Activity");
    activity.setType("init-dataflow");
    activity.withCoordinator(DataflowActivityCoordinator.class);
    activity.withActivityStepBuilder(DataflowInitActivityStepBuilder.class) ;
    return activity;
  }
  
  @Singleton
  static public class DataflowInitActivityStepBuilder implements ActivityStepBuilder {
    @Override
    public List<ActivityStep> build(Activity activity, Injector container) throws Exception {
      List<ActivityStep> steps = new ArrayList<>() ;
      steps.add(
          new ActivityStep().
          withType("init-streams").
          withExecutor(InitStreamsExecutor.class));
      return steps;
    }
  }
  
  @Singleton
  static public class InitStreamsExecutor implements ActivityStepExecutor {
    @Inject
    DataflowMasterService service;
    
    @Override
    public void execute(ActivityExecutionContext ctx, Activity activity, ActivityStep step) throws Exception {
      System.out.println("Init Stream Executor....");
    }
  }
}