package com.neverwinterdp.scribengin.dataflow.worker;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;

import com.neverwinterdp.es.log.OSMonitorLoggerService;
import com.neverwinterdp.module.AppContainer;
import com.neverwinterdp.module.DataflowWorkerModule;
import com.neverwinterdp.module.ServiceModuleContainer;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.vm.VMApp;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMConfig.ClusterEnvironment;
import com.neverwinterdp.yara.MetricPrinter;
import com.neverwinterdp.yara.MetricRegistry;


public class VMDataflowWorkerApp extends VMApp {
  private Logger logger  ;
  
  private DataflowTaskExecutorService dataflowTaskExecutorService;
  
  @Override
  public void run() throws Exception {
    final VMConfig vmConfig = getVM().getDescriptor().getVmConfig();
    logger = getVM().getLoggerFactory().getLogger(VMDataflowWorkerApp.class);
    
    AppContainer appContainer = getVM().getAppContainer();
    Map<String, String> moduleProps = new HashMap<String, String>();
    moduleProps.putAll(vmConfig.getHadoopProperties());
    if(vmConfig.getClusterEnvironment() ==  ClusterEnvironment.JVM) {
      moduleProps.put("cluster.environment", "jvm");
    } else {
      moduleProps.put("cluster.environment", "yarn");
    }
   
    appContainer.install(moduleProps, DataflowWorkerModule.NAME);
    ServiceModuleContainer dataflowWorkerModuleContainer = appContainer.getModule(DataflowWorkerModule.NAME);
    
    dataflowWorkerModuleContainer.getInstance(DataflowRegistry.class).addWorker(getVM().getDescriptor());
    dataflowTaskExecutorService = dataflowWorkerModuleContainer.getInstance(DataflowTaskExecutorService.class);
    addListener(new VMApp.VMAppTerminateEventListener() {
      @Override
      public void onEvent(VMApp vmApp, TerminateEvent terminateEvent) {
        try {
          if(terminateEvent == TerminateEvent.Shutdown) {
            dataflowTaskExecutorService.shutdown();
          } else if(terminateEvent == TerminateEvent.SimulateKill) {
            dataflowTaskExecutorService.simulateKill();
          } else if(terminateEvent == TerminateEvent.Kill) {
            System.exit(0);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
    //TODO: fix to use module
    dataflowWorkerModuleContainer.getInstance(OSMonitorLoggerService.class);
    try {
      dataflowTaskExecutorService.start();
      dataflowTaskExecutorService.waitForTerminated(500);
    } catch(InterruptedException ex) {
    } finally {
      StringBuilder out = new StringBuilder() ;
      MetricPrinter metricPrinter = new MetricPrinter(out) ;
      MetricRegistry mRegistry = dataflowWorkerModuleContainer.getInstance(MetricRegistry.class);
      metricPrinter.print(mRegistry);
      logger.info("\n" + out.toString());
    }
  }
}