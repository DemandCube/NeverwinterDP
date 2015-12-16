package com.neverwinterdp.scribengin.dataflow.master.activity;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.activity.ActivityBuilder;
import com.neverwinterdp.registry.activity.ActivityExecutionContext;
import com.neverwinterdp.registry.activity.ActivityStep;
import com.neverwinterdp.registry.activity.ActivityStepBuilder;
import com.neverwinterdp.registry.activity.ActivityStepExecutor;
import com.neverwinterdp.scribengin.dataflow.api.DataSetDescriptor;
import com.neverwinterdp.scribengin.dataflow.api.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.api.OperatorDescriptor;
import com.neverwinterdp.scribengin.dataflow.master.MasterService;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowTaskRegistry;
import com.neverwinterdp.scribengin.dataflow.registry.OperatorRegistry;
import com.neverwinterdp.scribengin.dataflow.registry.StreamRegistry;
import com.neverwinterdp.scribengin.dataflow.runtime.DataStreamOperatorDescriptor;
import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.Storage;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.StorageService;
import com.neverwinterdp.storage.sink.Sink;

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
      steps.add(new ActivityStep().withType("init-streams").withExecutor(InitStreamsExecutor.class));
      steps.add(new ActivityStep().withType("init-operators").withExecutor(InitOperatorExecutor.class));
      steps.add(new ActivityStep().withType("init-tasks").withExecutor(InitDataflowTaskExecutor.class));
      return steps;
    }
  }
  
  @Singleton
  static public class InitStreamsExecutor implements ActivityStepExecutor {
    @Inject
    MasterService service;
    
    @Override
    public void execute(ActivityExecutionContext ctx, Activity activity, ActivityStep step) throws Exception {
      DataflowRegistry dflRegistry = service.getDataflowRegistry();
      StreamRegistry streamRegistry = dflRegistry.getStreamRegistry();
      StorageService storageService = service.getStorageService();
      DataflowDescriptor dflConfig = dflRegistry.getConfigRegistry().getDataflowConfig();
      DataSetDescriptor streamConfig  = dflConfig.getStreamConfig();
      Map<String, StorageConfig> storageConfigs = streamConfig.getStreams();
      for(Map.Entry<String, StorageConfig> entry : storageConfigs.entrySet()) {
        String name = entry.getKey();
        StorageConfig storageConfig = entry.getValue();
        Storage storage = storageService.getStorage(storageConfig);
        storage.refresh();
        if(!storage.exists()) {
          storageConfig.setPartitionStream(streamConfig.getParallelism());
          storageConfig.setReplication(streamConfig.getReplication());
          storage.create();
          storage.refresh();
        }
        Sink sink = storage.getSink();
        List<PartitionStreamConfig> pConfigs = sink.getPartitionStreamConfigs();
        streamRegistry.create(name, storageConfig, pConfigs);
      }
    }
  }
  
  @Singleton
  static public class InitOperatorExecutor implements ActivityStepExecutor {
    @Inject
    MasterService service;
    
    @Override
    public void execute(ActivityExecutionContext ctx, Activity activity, ActivityStep step) throws Exception {
      DataflowRegistry dflRegistry = service.getDataflowRegistry();
      OperatorRegistry operatorRegistry = dflRegistry.getOperatorRegistry();
      DataflowDescriptor dflConfig = dflRegistry.getConfigRegistry().getDataflowConfig();
      Map<String, OperatorDescriptor> operators = dflConfig.getOperators();
      for(Map.Entry<String, OperatorDescriptor> entry : operators.entrySet()) {
        String name = entry.getKey();
        OperatorDescriptor config = entry.getValue();
        operatorRegistry.create(name, config);
      }
    }
  }
  
  @Singleton
  static public class InitDataflowTaskExecutor implements ActivityStepExecutor {
    @Inject
    MasterService service;
    
    @Override
    public void execute(ActivityExecutionContext ctx, Activity activity, ActivityStep step) throws Exception {
      DataflowRegistry dflRegistry = service.getDataflowRegistry();
      DataflowDescriptor dflConfig = dflRegistry.getConfigRegistry().getDataflowConfig();
      Map<String, OperatorDescriptor> operators = dflConfig.getOperators();
      for(Map.Entry<String, OperatorDescriptor> entry : operators.entrySet()) {
        String opName = entry.getKey();
        OperatorDescriptor opConfig = entry.getValue();
        createTasks(opName, opConfig);
      }
    }
    
    void createTasks(String opName, OperatorDescriptor opConfig) throws RegistryException {
      DecimalFormat SEQ_ID_FORMATTER = new DecimalFormat("0000");
      StreamRegistry streamRegistry = service.getDataflowRegistry().getStreamRegistry();
      DataflowTaskRegistry taskRegistry = service.getDataflowRegistry().getTaskRegistry();
      for(String input : opConfig.getInputs()) {
        List<PartitionStreamConfig> pConfigs = streamRegistry.getStreamInputPartitions(input);
        for(int i = 0; i < pConfigs.size(); i++) {
          PartitionStreamConfig pConfig = pConfigs.get(i);
          String taskId =  opName + ":" + input + "-" + SEQ_ID_FORMATTER.format(pConfig.getPartitionStreamId());
          DataStreamOperatorDescriptor taskConfig = new DataStreamOperatorDescriptor();
          taskConfig.setTaskId(taskId);
          taskConfig.setOperatorName(opName);
          taskConfig.setInput(input);
          taskConfig.setInputPartitionId(pConfig.getPartitionStreamId());
          taskConfig.setOutputs(opConfig.getOutputs());
          taskConfig.setOperator(opConfig.getOperator());
          taskRegistry.offer(taskConfig);
        }
      }
    }
  }
}