package com.neverwinterdp.scribengin.dataflow.master;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;

import com.neverwinterdp.module.AppContainer;
import com.neverwinterdp.module.DataflowServiceModule;
import com.neverwinterdp.module.ESOSMonitorLoggerModule;
import com.neverwinterdp.module.ServiceModuleContainer;
import com.neverwinterdp.registry.RefNode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.election.LeaderElection;
import com.neverwinterdp.registry.election.LeaderElectionListener;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.vm.VMApp;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMConfig.ClusterEnvironment;

public class VMMasterApp extends VMApp {
  private Logger                logger;
  private String                dataflowRegistryPath;
  private LeaderElection        election;
  private MasterService dataflowService;
  private ServiceRunnerThread   serviceRunnerThread;

  @Override
  public void run() throws Exception {
    logger = getVM().getLoggerFactory().getLogger(VMMasterApp.class);
    logger.info("Start run()");
    Registry registry = getVM().getVMRegistry().getRegistry();
    VMConfig vmConfig = getVM().getDescriptor().getVmConfig();
    dataflowRegistryPath = vmConfig.getProperties().get("dataflow.registry.path");
    RefNode leaderRefNode = new RefNode(getVM().getDescriptor().getRegistryPath());
    election = new LeaderElection(registry, dataflowRegistryPath + "/master/leader", leaderRefNode) ;
    election.setListener(new MasterLeaderElectionListener());
    election.start();
    try {
      waitForTerminate();
      logger.info("Finish waitForTerminate()");
    } catch(InterruptedException ex) {
    } finally {
      if(election != null && election.getLeaderId() != null) {
        election.stop();
      }
    }
  }
 
  class MasterLeaderElectionListener implements LeaderElectionListener {
    @Override
    public void onElected() {
      final Registry registry = getVM().getVMRegistry().getRegistry();
      System.err.println("VMDataflowServiceApp: on elected " + getVM().getDescriptor().getId() + "with registry " + registry.hashCode()) ;
      try {
        if(registry.exists(dataflowRegistryPath + "/status")) {
          DataflowLifecycleStatus currentStatus =  DataflowRegistry.getStatus(registry, dataflowRegistryPath);
          if(currentStatus == DataflowLifecycleStatus.FINISH) {
            terminate(TerminateEvent.Shutdown);
            return;
          }
        }
        VMConfig vmConfig = getVM().getDescriptor().getVmConfig();
        AppContainer appContainer = getVM().getAppContainer();
        Map<String, String> esLoggerModuleProps = new HashMap<String, String>();
        appContainer.install(esLoggerModuleProps, ESOSMonitorLoggerModule.NAME);
        
        Map<String, String> moduleProps = new HashMap<String, String>();
        moduleProps.putAll(vmConfig.getHadoopProperties());
        if(vmConfig.getClusterEnvironment() ==  ClusterEnvironment.JVM) {
          moduleProps.put("cluster.environment", "jvm");
        } else {
          moduleProps.put("cluster.environment", "yarn");
        }
       
        appContainer.install(moduleProps, DataflowServiceModule.NAME);
        ServiceModuleContainer dataflowServiceModuleContainer = appContainer.getModule(DataflowServiceModule.NAME);

        RefNode leaderRefNode = new RefNode(getVM().getDescriptor().getRegistryPath());
        registry.setData(dataflowRegistryPath + "/master/leader", leaderRefNode);
        dataflowService = dataflowServiceModuleContainer.getInstance(MasterService.class);
        serviceRunnerThread = new ServiceRunnerThread(dataflowService);
        serviceRunnerThread.start();
      } catch(Exception e) {
        e.printStackTrace();
      }
    }
  }
  
  public class ServiceRunnerThread extends Thread {
    MasterService service;
    
    ServiceRunnerThread(MasterService service) {
      this.service = service;
    }
    
    public void run() {
      try {
        service.init();
        service.run();
        service. waitForTermination();
      } catch (Exception e) {
        logger.error("Error: ", e);
      } finally {
        terminate(TerminateEvent.Shutdown);
      }
    }
  }
}