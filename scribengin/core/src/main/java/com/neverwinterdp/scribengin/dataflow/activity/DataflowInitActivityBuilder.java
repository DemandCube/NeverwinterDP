package com.neverwinterdp.scribengin.dataflow.activity;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.activity.ActivityBuilder;
import com.neverwinterdp.registry.activity.ActivityCoordinator;
import com.neverwinterdp.registry.activity.ActivityExecutionContext;
import com.neverwinterdp.registry.activity.ActivityStep;
import com.neverwinterdp.registry.activity.ActivityStepBuilder;
import com.neverwinterdp.registry.activity.ActivityStepExecutor;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;
import com.neverwinterdp.scribengin.dataflow.service.DataflowService;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.sink.SinkFactory;
import com.neverwinterdp.scribengin.storage.source.Source;
import com.neverwinterdp.scribengin.storage.source.SourceFactory;
import com.neverwinterdp.scribengin.storage.source.SourceStream;

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
          withType("init-dataflow-task").
          withExecutor(InitDataflowTaskExecutor.class));
      return steps;
    }
  }
  
  @Singleton
  static public class InitDataflowTaskExecutor implements ActivityStepExecutor {
    @Inject
    private DataflowService service ;

    @Override
    public void execute(ActivityExecutionContext ctx, Activity activity, ActivityStep step) throws Exception {
      DataflowDescriptor dataflowDescriptor = service.getDataflowRegistry().getDataflowDescriptor();
      SourceFactory sourceFactory = service.getSourceFactory();
      SinkFactory sinkFactory = service.getSinkFactory() ;

      Source source    = sourceFactory.create(dataflowDescriptor.getSourceDescriptor()) ;
      Map<String, Sink> sinks = new HashMap<String, Sink>();
      for(Map.Entry<String, StorageDescriptor> entry : dataflowDescriptor.getSinkDescriptors().entrySet()) {
        Sink sink = sinkFactory.create(entry.getValue());
        sinks.put(entry.getKey(), sink);
      }

      DecimalFormat seqIdFormatter = new DecimalFormat("00000");
      
      SourceStream[] sourceStream = source.getStreams();
      for(int i = 0; i < sourceStream.length; i++) {
        String taskId =  "task-" + seqIdFormatter.format(i);
        DataflowTaskDescriptor descriptor = new DataflowTaskDescriptor();
        descriptor.setTaskId(taskId);
        descriptor.setScribe(dataflowDescriptor.getScribe());
        descriptor.setSourceStreamDescriptor(sourceStream[i].getDescriptor());
        for(Map.Entry<String, Sink> entry : sinks.entrySet()) {
          descriptor.add(entry.getKey(), entry.getValue().newStream().getDescriptor());
        }
        service.addAvailableTask(descriptor);
      }
    }
  }
}