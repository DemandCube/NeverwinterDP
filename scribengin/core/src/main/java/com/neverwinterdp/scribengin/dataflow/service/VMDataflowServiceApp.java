package com.neverwinterdp.scribengin.dataflow.service;

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

public class VMDataflowServiceApp extends VMApp {
  private Logger              logger;
  private String              dataflowRegistryPath;
  private LeaderElection      election;
  private DataflowService     dataflowService;
  private ServiceRunnerThread serviceRunnerThread;

  @Override
  public void run() throws Exception {
    logger = getVM().getLoggerFactory().getLogger(VMDataflowServiceApp.class);
    logger.info("Start run()");
    VMConfig vmConfig = getVM().getDescriptor().getVmConfig();
    dataflowRegistryPath = vmConfig.getProperties().get("dataflow.registry.path");
    election = new LeaderElection(getVM().getVMRegistry().getRegistry(), dataflowRegistryPath + "/master/leader") ;
    election.setListener(new MasterLeaderElectionListener());
    election.start();
    try {
      waitForTerminate();
      logger.info("Finish waitForTerminate()()");
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
      try {
        final Registry registry = getVM().getVMRegistry().getRegistry();
        if(registry.exists(dataflowRegistryPath + "/status") && DataflowLifecycleStatus.FINISH == DataflowRegistry.getStatus(registry, dataflowRegistryPath)) {
          terminate(TerminateEvent.Shutdown);
          return;
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

        RefNode leaderRefNode = new RefNode();
        leaderRefNode.setPath(getVM().getDescriptor().getRegistryPath());
        registry.setData(dataflowRegistryPath + "/master/leader", leaderRefNode);
        dataflowService = dataflowServiceModuleContainer.getInstance(DataflowService.class);
        serviceRunnerThread = new ServiceRunnerThread(dataflowService);
        serviceRunnerThread.start();
      } catch(Exception e) {
        e.printStackTrace();
      }
    }
  }
  
  public class ServiceRunnerThread extends Thread {
    DataflowService service;
    
    ServiceRunnerThread(DataflowService service) {
      this.service = service;
    }
    
    public void run() {
      try {
        service.run();
        service. waitForTermination(this);
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        terminate(TerminateEvent.Shutdown);
      }
    }
  }
}