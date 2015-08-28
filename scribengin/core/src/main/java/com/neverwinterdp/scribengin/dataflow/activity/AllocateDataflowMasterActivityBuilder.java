package com.neverwinterdp.scribengin.dataflow.activity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.SequenceIdTracker;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.activity.ActivityBuilder;
import com.neverwinterdp.registry.activity.ActivityCoordinator;
import com.neverwinterdp.registry.activity.ActivityExecutionContext;
import com.neverwinterdp.registry.activity.ActivityStep;
import com.neverwinterdp.registry.activity.ActivityStepBuilder;
import com.neverwinterdp.registry.activity.ActivityStepExecutor;
import com.neverwinterdp.scribengin.activity.ScribenginActivityStepWorkerService;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.service.VMDataflowServiceApp;
import com.neverwinterdp.scribengin.service.ScribenginService;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;

public class AllocateDataflowMasterActivityBuilder extends ActivityBuilder {
  
  public Activity build(String dataflowPath) {
    Activity activity = new Activity();
    activity.setDescription("Allocate Dataflow Master Activity");
    activity.setType("allocate-dataflow-master");
    activity.attribute("dataflow.path", dataflowPath);
    activity.withCoordinator(AllocateDataflowMasterActivityCoordinator.class);
    activity.withActivityStepBuilder(AllocateDataflowMasterActivityStepBuilder.class) ;
    return activity;
  }
  
  @Singleton
  static public class AllocateDataflowMasterActivityStepBuilder implements ActivityStepBuilder {
    @Override
    public List<ActivityStep> build(Activity activity, Injector container) throws Exception {
      List<ActivityStep> steps = new ArrayList<>() ;
      steps.add(
        new ActivityStep().
        withType("allocate-dataflow-master").
        withExecutor(AllocateDataflowMasterStepExecutor.class));
      return steps;
    }
  }
  
  @Singleton
  static public class AllocateDataflowMasterActivityCoordinator extends ActivityCoordinator {
    @Inject
    ScribenginActivityStepWorkerService activityStepWorkerService;

    @Override
    protected <T> void execute(ActivityExecutionContext context, Activity activity, ActivityStep step) throws Exception {
      activityStepWorkerService.exectute(context, activity, step);
    }
  }
  
  @Singleton
  static public class AllocateDataflowMasterStepExecutor implements ActivityStepExecutor {
    @Inject
    private Registry registry ;
    
    @Inject
    private VMConfig vmConfig;
    
    @Override
    public void execute(ActivityExecutionContext context, Activity activity, ActivityStep step) throws Exception {
      SequenceIdTracker idTracker = 
        new SequenceIdTracker(registry, ScribenginService.DATAFLOW_MASTER_ID_TRACKER);
      String dataflowPath = activity.attribute("dataflow.path");
      Node dataflowNode   = registry.get(dataflowPath) ;
      DataflowDescriptor descriptor = dataflowNode.getDataAs(DataflowDescriptor.class);
      
      List<VMDescriptor> masters = 
          ScribenginService.getDataflowMasterDescriptors(registry, dataflowPath);
      for(int i = masters.size(); i < 1; i++) {
        String masterId = descriptor.getId() + "-master-" + idTracker.nextSeqId();
        allocate(dataflowNode, descriptor, masterId);
      }
    }
    
    void allocate(Node dataflowNode, DataflowDescriptor descriptor, String masterId) throws Exception {
      String dataflowAppHome = descriptor.getDataflowAppHome();
      VMConfig dfVMConfig = new VMConfig() ;
      if(dataflowAppHome != null) {
        dfVMConfig.setAppHome(dataflowAppHome);
        dfVMConfig.addVMResource("dataflow.libs", dataflowAppHome + "/libs");
      }
      dfVMConfig.setClusterEnvironment(vmConfig.getClusterEnvironment());
      dfVMConfig.setName(masterId);
      dfVMConfig.setRoles(Arrays.asList("dataflow-master"));
      dfVMConfig.setRegistryConfig(registry.getRegistryConfig());
      dfVMConfig.setVmApplication(VMDataflowServiceApp.class.getName());
      dfVMConfig.addProperty("dataflow.registry.path", dataflowNode.getPath());
      dfVMConfig.setHadoopProperties(vmConfig.getHadoopProperties());
      VMClient vmClient = new VMClient(registry);
      VMDescriptor vmDescriptor = vmClient.allocate(dfVMConfig);
    }
  }
}
