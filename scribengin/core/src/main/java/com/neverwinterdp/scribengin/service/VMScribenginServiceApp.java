package com.neverwinterdp.scribengin.service;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;

import com.neverwinterdp.module.AppContainer;
import com.neverwinterdp.module.ESOSMonitorLoggerModule;
import com.neverwinterdp.module.ScribenginServiceModule;
import com.neverwinterdp.module.ServiceModuleContainer;
import com.neverwinterdp.registry.RefNode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.election.LeaderElection;
import com.neverwinterdp.registry.election.LeaderElectionListener;
import com.neverwinterdp.scribengin.event.ScribenginShutdownEventListener;
import com.neverwinterdp.vm.VMApp;

public class VMScribenginServiceApp extends VMApp {
  private Logger logger ;
  private LeaderElection election ;
  private ServiceModuleContainer scribenginServiceModuleContainer;
  private ScribenginService scribenginService;
  
  public ScribenginService getScribenginService() { return this.scribenginService ; }
  
  @Override
  public void run() throws Exception {
    logger = getVM().getLoggerFactory().getLogger(VMScribenginServiceApp.class);
    Registry registry = getVM().getVMRegistry().getRegistry();
    getVM().getVMRegistry().getRegistry().createIfNotExist(ScribenginService.LEADER_PATH) ;
    RefNode masterVMRef = new RefNode(getVM().getDescriptor().getRegistryPath()) ;
    election = new LeaderElection(getVM().getVMRegistry().getRegistry(), ScribenginService.LEADER_PATH, masterVMRef) ;
    election.setListener(new MasterLeaderElectionListener());
    election.start();
    
    ScribenginShutdownEventListener shutdownListener = new ScribenginShutdownEventListener(registry) {
      @Override
      public void onShutdownEvent() { terminate(TerminateEvent.Shutdown); }
    };

    try {
      waitForTerminate();
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
        AppContainer appContainer = getVM().getAppContainer();
        
        Map<String, String> esLoggerModuleProps = new HashMap<String, String>();
        appContainer.install(esLoggerModuleProps, ESOSMonitorLoggerModule.NAME);
        
        appContainer.install(new HashMap<String, String>(), ScribenginServiceModule.NAME);
        scribenginServiceModuleContainer = appContainer.getModule(ScribenginServiceModule.NAME);
        
        //TODO: fix to use module
        //appContainer.getInstance(OSMonitorLoggerService.class);
        
        scribenginService = scribenginServiceModuleContainer.getInstance(ScribenginService.class);
        RefNode refNode = new RefNode();
        refNode.setPath(getVM().getDescriptor().getRegistryPath());
        registry.setData(ScribenginService.LEADER_PATH, refNode);
      } catch(Exception e) {
        logger.error("Elected SCribengin Master Error:", e);
      }
    }
  }
}