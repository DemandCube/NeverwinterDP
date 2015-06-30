package com.neverwinterdp.vm.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.neverwinterdp.module.AppContainer;
import com.neverwinterdp.module.ServiceModuleContainer;
import com.neverwinterdp.module.VMServiceModule;
import com.neverwinterdp.registry.RefNode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.election.LeaderElection;
import com.neverwinterdp.registry.election.LeaderElectionListener;
import com.neverwinterdp.vm.VMApp;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMConfig.ClusterEnvironment;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.environment.jvm.JVMVMServicePlugin;
import com.neverwinterdp.vm.environment.yarn.YarnVMServicePlugin;
import com.neverwinterdp.vm.event.VMShutdownEventListener;


public class VMServiceApp extends VMApp {
  private LeaderElection election ;
  
  private ServiceModuleContainer vmServiceModuleContainer;
  private VMShutdownEventListener shutdownListener;
  
  public VMService getVMService() { return vmServiceModuleContainer.getInstance(VMService.class); }
 
  @Override
  public void run() throws Exception {
    election = new LeaderElection(getVM().getVMRegistry().getRegistry(), VMService.LEADER_PATH) ;
    election.setListener(new VMServiceLeaderElectionListener());
    election.start();
    
    Registry registry = getVM().getVMRegistry().getRegistry();
    
    shutdownListener = new VMShutdownEventListener(registry) {
      @Override
      public void onShutdownEvent() throws Exception {
        terminate(TerminateEvent.Shutdown);
      }
    };

    try {
      waitForTerminate();
    } catch(InterruptedException ex) {
    } finally {
      if(election != null && election.getLeaderId() != null) {
        election.stop();
      }
      VMService vmService = getVMService();
      if(vmService != null) {
        //TODO: should check to make sure the resource are clean before destroy the service
        Thread.sleep(3000);
        vmService.shutdown();
      }
    }
  }
  
  public void startVMService() {
    try {
      VMConfig vmConfig = getVM().getDescriptor().getVmConfig();
      final Registry registry = getVM().getVMRegistry().getRegistry();
      RefNode refNode = new RefNode();
      refNode.setPath(getVM().getDescriptor().getRegistryPath());
      registry.setData(VMService.LEADER_PATH, refNode);
      AppContainer appContainer = getVM().getAppContainer();
      Map<String, String> moduleProps = new HashMap<>() ;
      if(vmConfig.getClusterEnvironment() ==  ClusterEnvironment.JVM) {
        moduleProps.put("module.vm.vmservice.plugin", JVMVMServicePlugin.class.getName());
      } else {
        moduleProps.put("module.vm.vmservice.plugin", YarnVMServicePlugin.class.getName());
      }
      appContainer.install(moduleProps, VMServiceModule.NAME);
      vmServiceModuleContainer = appContainer.getModule(VMServiceModule.NAME);
     
      VMService vmService = vmServiceModuleContainer.getInstance(VMService.class);
      vmService.setStatus(VMService.Status.RUNNING);
      List<VMDescriptor> vmDescriptors = vmService.getActiveVMDescriptors();
      for(VMDescriptor sel : vmDescriptors) {
        if(vmService.isRunning(sel)) vmService.watch(sel);
        else vmService.unregister(sel);
      }
    } catch(Throwable e) {
      e.printStackTrace();
    }
  }
  
  class VMServiceLeaderElectionListener implements LeaderElectionListener {
    @Override
    public void onElected() {
      startVMService();
    }
  }
  
 
}