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
import com.neverwinterdp.registry.task.TaskRegistry;
import com.neverwinterdp.scribengin.dataflow.config.DataflowConfig;
import com.neverwinterdp.scribengin.dataflow.config.OperatorConfig;
import com.neverwinterdp.scribengin.dataflow.master.MasterService;
import com.neverwinterdp.scribengin.dataflow.operator.OperatorTaskConfig;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowTaskRegistry;
import com.neverwinterdp.scribengin.dataflow.registry.OperatorRegistry;
import com.neverwinterdp.scribengin.dataflow.registry.StreamRegistry;
import com.neverwinterdp.scribengin.storage.PartitionConfig;
import com.neverwinterdp.scribengin.storage.Storage;
import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.StorageService;

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
      System.out.println("Init Stream Executor....");
      DataflowRegistry dflRegistry = service.getDataflowRegistry();
      StreamRegistry streamRegistry = dflRegistry.getStreamRegistry();
      StorageService storageService = service.getStorageService();
      DataflowConfig dflConfig = dflRegistry.getConfigRegistry().getDataflowConfig();
      Map<String, StorageConfig> storageDescriptors = dflConfig.getStreams();
      for(Map.Entry<String, StorageConfig> entry : storageDescriptors.entrySet()) {
        String name = entry.getKey();
        StorageConfig storageConfig = entry.getValue();
        Storage storage = storageService.getStorage(storageConfig);
        storage.refresh();
        if(storage.exists()) storage.drop();
        storage.create(storageConfig.getPartition(), storageConfig.getReplication());
        storage.refresh();
        List<PartitionConfig> pConfigs = storage.getPartitionConfigs();
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
      System.out.println("Init Operator Executor....");
      DataflowRegistry dflRegistry = service.getDataflowRegistry();
      OperatorRegistry operatorRegistry = dflRegistry.getOperatorRegistry();
      DataflowConfig dflConfig = dflRegistry.getConfigRegistry().getDataflowConfig();
      Map<String, OperatorConfig> operators = dflConfig.getOperators();
      for(Map.Entry<String, OperatorConfig> entry : operators.entrySet()) {
        String name = entry.getKey();
        OperatorConfig config = entry.getValue();
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
      System.out.println("Init Dataflow Task Executor....");
      DataflowRegistry dflRegistry = service.getDataflowRegistry();
      DataflowConfig dflConfig = dflRegistry.getConfigRegistry().getDataflowConfig();
      Map<String, OperatorConfig> operators = dflConfig.getOperators();
      for(Map.Entry<String, OperatorConfig> entry : operators.entrySet()) {
        String opName = entry.getKey();
        OperatorConfig opConfig = entry.getValue();
        createTasks(opName, opConfig);
      }
    }
    
    void createTasks(String opName, OperatorConfig opConfig) throws RegistryException {
      DecimalFormat SEQ_ID_FORMATTER = new DecimalFormat("0000");
      StreamRegistry streamRegistry = service.getDataflowRegistry().getStreamRegistry();
      DataflowTaskRegistry taskRegistry = service.getDataflowRegistry().getTaskRegistry();
      for(String input : opConfig.getInputs()) {
        List<PartitionConfig> pConfigs = streamRegistry.getStreamInputPartitions(input);
        for(int i = 0; i < pConfigs.size(); i++) {
          PartitionConfig pConfig = pConfigs.get(i);
          String taskId =  opName + ":" + input + "-" + SEQ_ID_FORMATTER.format(pConfig.getPartitionId());
          OperatorTaskConfig taskConfig = new OperatorTaskConfig();
          taskConfig.setTaskId(taskId);
          taskConfig.setOperatorName(opName);
          taskConfig.setInput(input);
          taskConfig.setInputPartitionId(pConfig.getPartitionId());
          taskConfig.setOutputs(opConfig.getOutputs());
          taskConfig.setOperator(opConfig.getScribe());
          taskRegistry.offer(taskId, taskConfig);
        }
      }
    }
  }
}