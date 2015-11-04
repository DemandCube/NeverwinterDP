package com.neverwinterdp.scribengin.dataflow.worker;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;

import com.neverwinterdp.module.AppContainer;
import com.neverwinterdp.module.DataflowWorkerModule;
import com.neverwinterdp.module.ServiceModuleContainer;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.vm.VMApp;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMConfig.ClusterEnvironment;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.yara.MetricRegistry;

public class VMWorkerApp extends VMApp {
  private Logger logger  ;
  
  private WorkerService dataflowTaskExecutorService;
  
  @Override
  public void run() throws Exception {
    final VMDescriptor vmDescriptor = getVM().getDescriptor();
    final VMConfig vmConfig = vmDescriptor.getVmConfig();
    logger = getVM().getLoggerFactory().getLogger(VMWorkerApp.class);
    
    AppContainer appContainer = getVM().getAppContainer();
    Map<String, String> esLoggerModuleProps = new HashMap<String, String>();
    //appContainer.install(esLoggerModuleProps, ESOSMonitorLoggerModule.NAME);
    
    Map<String, String> workerModuleProps = new HashMap<String, String>();
    workerModuleProps.putAll(vmConfig.getHadoopProperties());
    if(vmConfig.getClusterEnvironment() ==  ClusterEnvironment.JVM) {
      workerModuleProps.put("cluster.environment", "jvm");
    } else {
      workerModuleProps.put("cluster.environment", "yarn");
    }
    
    workerModuleProps.put("kafka.zk.connects", vmConfig.getRegistryConfig().getConnect());
    appContainer.install(workerModuleProps, DataflowWorkerModule.NAME);
    ServiceModuleContainer workerModuleContainer = appContainer.getModule(DataflowWorkerModule.NAME);
    
    DataflowRegistry dflRegistry = workerModuleContainer.getInstance(DataflowRegistry.class);
    dflRegistry.getWorkerRegistry().addWorker(getVM().getDescriptor());
    MetricRegistry mRegistry = workerModuleContainer.getInstance(MetricRegistry.class);
    dflRegistry.getWorkerRegistry().createMetric(vmDescriptor.getId(), mRegistry);
    
    dataflowTaskExecutorService = workerModuleContainer.getInstance(WorkerService.class);
    addListener(new VMApp.VMAppTerminateEventListener() {
      @Override
      public void onEvent(VMApp vmApp, TerminateEvent terminateEvent) {
        try {
          if(terminateEvent == TerminateEvent.Shutdown) {
            dataflowTaskExecutorService.shutdown();
          } else if(terminateEvent == TerminateEvent.SimulateKill) {
            logger.info("Execute the simulate kill event");
            dataflowTaskExecutorService.simulateKill();
          } else if(terminateEvent == TerminateEvent.Kill) {
            logger.info("Execute the kill event with Runtime.getRuntime().halt(0)");
            Runtime.getRuntime().halt(0);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
    
    dataflowTaskExecutorService.init();
    dataflowTaskExecutorService.run();
    dataflowTaskExecutorService.waitForTermination();
    dflRegistry.
      getWorkerRegistry().
      setWorkerStatus(getVM().getDescriptor().getId(), DataflowWorkerStatus.TERMINATED);
    System.out.println("VMWorkerApp Done !!!!!!!!!");
  }
}