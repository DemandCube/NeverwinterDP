package com.neverwinterdp.scribengin.dataflow.runtime.master;

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
import com.neverwinterdp.vm.VMApp;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMConfig.ClusterEnvironment;

public class VMMasterApp extends VMApp {
  private Logger              logger;
  private String              dataflowRegistryPath;
  private LeaderElection      election;
  private MasterService       masterService;
  private ServiceRunnerThread serviceRunnerThread;

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
      System.err.println("VMDataflowServiceApp: on elected " + getVM().getDescriptor().getVmId() + " with registry " + registry.hashCode()) ;
      try {
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
       
        moduleProps.put("kafka.zk.connects", vmConfig.getRegistryConfig().getConnect());
        appContainer.install(moduleProps, DataflowServiceModule.NAME);
        ServiceModuleContainer dataflowServiceModuleContainer = appContainer.getModule(DataflowServiceModule.NAME);

        RefNode leaderRefNode = new RefNode(getVM().getDescriptor().getRegistryPath());
        registry.setData(dataflowRegistryPath + "/master/leader", leaderRefNode);
        masterService = dataflowServiceModuleContainer.getInstance(MasterService.class);
        
        addListener(new VMApp.VMAppTerminateEventListener() {
          @Override
          public void onEvent(VMApp vmApp, TerminateEvent terminateEvent) {
            try {
              if(terminateEvent == TerminateEvent.SimulateKill) {
                logger.info("Execute the simulate kill event");
                masterService.simulateKill();
              } else if(terminateEvent == TerminateEvent.Kill) {
                logger.info("Execute the kill event with Runtime.getRuntime().halt(0)");
                Runtime.getRuntime().halt(0);
              }
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        });
        
        
        serviceRunnerThread = new ServiceRunnerThread(masterService);
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
        System.out.println("VMMasterApp Done !!!!!!!!!");
      } catch (Exception e) {
        logger.error("Error: ", e);
      } finally {
        terminate(TerminateEvent.Shutdown);
      }
    }
  }
}