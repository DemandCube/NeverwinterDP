package com.neverwinterdp.scribengin.dataflow.runtime.master.activity;

import java.util.ArrayList;
import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.SequenceIdTracker;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.activity.ActivityBuilder;
import com.neverwinterdp.registry.activity.ActivityExecutionContext;
import com.neverwinterdp.registry.activity.ActivityStep;
import com.neverwinterdp.registry.activity.ActivityStepBuilder;
import com.neverwinterdp.registry.activity.ActivityStepExecutor;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.runtime.master.MasterService;
import com.neverwinterdp.scribengin.dataflow.runtime.worker.DataflowWorkerStatus;
import com.neverwinterdp.scribengin.dataflow.runtime.worker.VMWorkerApp;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;

public class AllocateWorkerActivityBuilder extends ActivityBuilder {
  public Activity build() {
    Activity activity = new Activity();
    activity.setDescription("Allocate Dataflow Worker Activity");
    activity.setType("allocate-dataflow-worker");
    activity.withCoordinator(DataflowActivityCoordinator.class);
    activity.withActivityStepBuilder(AllocateDataflowWorkerActivityStepBuilder.class) ;
    return activity;
  }
  
  @Singleton
  static public class AllocateDataflowWorkerActivityStepBuilder implements ActivityStepBuilder {
    @Inject
    private Registry registry ;
    
    @Override
    public List<ActivityStep> build(Activity activity, Injector container) throws Exception {
      List<ActivityStep> steps = new ArrayList<>() ;
      steps.add(createAllocateDataflowWorkerStep(registry));
      steps.add(
        new ActivityStep().
        withType("wait-for-worker-run-status").
        withExecutor(WaitForWorkerRunningStatus.class));
      return steps;
    }
    
    static public ActivityStep createAllocateDataflowWorkerStep(Registry registry) throws Exception {
      ActivityStep step = new ActivityStep().
      withType("allocate-dataflow-worker").
      withExecutor(AllocateDataflowWorkerStepExecutor.class);
      return step;
    }
  }
  
  @Singleton
  static public class AllocateDataflowWorkerStepExecutor implements ActivityStepExecutor {
    @Inject
    private MasterService service ;
    
    @Override
    public void execute(ActivityExecutionContext context, Activity activity, ActivityStep step) throws Exception {
      DataflowRegistry dflRegistry = service.getDataflowRegistry();
      DataflowDescriptor dflConfig = dflRegistry.getConfigRegistry().getDataflowDescriptor();
      List<String> activeWorkers = dflRegistry.getWorkerRegistry().getActiveWorkerIds();
      //TODO: fix this hack
      int numOfInstanceToAllocate = 1;
      if(activeWorkers.size() == 0) numOfInstanceToAllocate = dflConfig.getWorker().getNumOfInstances();

      for(int i = 0; i < numOfInstanceToAllocate; i++) {
        String workerId = dflConfig.getId() + "-worker-" + dflRegistry.getWorkerIdTracker().nextSeqId();
        allocate(dflRegistry, dflConfig, workerId);
      }
    }
    
    private void allocate(DataflowRegistry dflRegistry, DataflowDescriptor dflConfig, String workerId) throws Exception {
      Registry registry = dflRegistry.getRegistry();
      RegistryConfig registryConfig = registry.getRegistryConfig();
      VMConfig vmConfig = new VMConfig();
      vmConfig.
        setClusterEnvironment(service.getVMConfig().getClusterEnvironment()).
        setVmId(workerId).
        addRoles("dataflow-worker").
        setRequestCpuCores(dflConfig.getWorker().getCpuCores()).
        setRequestMemory(dflConfig.getWorker().getMemory()).
        setRegistryConfig(registryConfig).
        setVmApplication(VMWorkerApp.class.getName()).
        addProperty("dataflow.registry.path", dflRegistry.getDataflowPath()).
        setHadoopProperties(service.getVMConfig().getHadoopProperties()).
        setLog4jConfigUrl(dflConfig.getWorker().getLog4jConfigUrl()).
        setEnableGCLog(dflConfig.getWorker().isEnableGCLog());

      String dataflowAppHome = dflConfig.getDataflowAppHome();
      if(dataflowAppHome != null) {
        vmConfig.setDfsAppHome(dataflowAppHome);
        vmConfig.addVMResource("dataflow.libs", dataflowAppHome + "/libs");
      }

      VMClient vmClient = new VMClient(registry);
      VMDescriptor vmDescriptor = vmClient.allocate(vmConfig, 90000);
      service.addWorker(vmDescriptor);
    }
  }
  
  @Singleton
  static public class WaitForWorkerRunningStatus implements ActivityStepExecutor {
    @Inject
    private MasterService service ;
    
    @Override
    public void execute(ActivityExecutionContext ctx, Activity activity, ActivityStep step) throws Exception {
      DataflowRegistry dflRegistry = service.getDataflowRegistry();
      DataflowDescriptor dflConfig = dflRegistry.getConfigRegistry().getDataflowDescriptor();
      long maxWait = dflConfig.getWorker().getMaxWaitForRunningStatus();
      dflRegistry.
        getWorkerRegistry().
        waitForWorkerStatus(DataflowWorkerStatus.RUNNING, 1000, maxWait);
    }
  }
}
