
package com.neverwinterdp.vm.environment.yarn;

import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mycila.jmx.annotation.JmxBean;
import com.neverwinterdp.vm.VMConfig;

@Singleton
@JmxBean("role=vm-manager, type=YarnManager, name=AsynYarnManager")
public class SyncYarnManager extends YarnManager {
  private AMRMClient<ContainerRequest> amrmClient ;
  
  private AtomicInteger countContainerRequest = new AtomicInteger();
  
  
  public AMRMClient<ContainerRequest> getAMRMClient() { return this.amrmClient ; }
  
  
  @Inject
  public void onInit(VMConfig vmConfig) throws Exception {
    logger.info("Start init(VMConfig vmConfig)");
    try {
      super.onInit(vmConfig);
      amrmClient = AMRMClient.createAMRMClient();
      amrmClient.init(conf);
      amrmClient.start();
      // Register with RM
      String appHostName = InetAddress.getLocalHost().getHostAddress()  ;
      RegisterApplicationMasterResponse registerResponse = amrmClient.registerApplicationMaster(appHostName, 0, "");
    } catch(Throwable t) {
      logger.error("Error: " , t);
      t.printStackTrace();
      throw t;
    }
    logger.info("Finish init(VMConfig vmConfig)");
  }

  public void onDestroy()  {
    logger.info("Start onDestroy()");
    try {
      if(amrmClient != null) {
        amrmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
        amrmClient.stop();
        amrmClient.close(); 
      }
      super.onDestroy();
    } catch(Exception ex) {
      logger.error("Cannot destroy YarnManager properly", ex);
    }
    logger.info("Finish onDestroy()");
  }
  
  synchronized public void add(VMRequest containerReq, ContainerRequestCallback callback) throws Exception {
    logger.info("Start add count = " + countContainerRequest.incrementAndGet());
    containerReq.setCallback(callback);
    Container container = null ;
    for(int i = 0; i < 1; i++) {
      container = allocateAndRun(containerReq, 60000);
      if(container != null) break;
    }
    if(container == null) {
      throw new Exception("Cannot allocate the container");
    }
    logger.info("Finish add");
  }
  
  private Container allocateAndRun(VMRequest vmReq, long timeout) throws Exception {
    vmReq.reset();
    ContainerRequest containerReq = vmReq.getContainerRequest();
    amrmClient.addContainerRequest(containerReq);
    long stopTime = System.currentTimeMillis() + timeout;
    int retry = 0;
    Container selContainer = null ;
    while (selContainer == null && System.currentTimeMillis() < stopTime) {
      retry++ ;
      AllocateResponse response = amrmClient.allocate(0 /*progress indicator*/);
      response.getCompletedContainersStatuses();
      response.getUpdatedNodes();
      List<Container> containers = response.getAllocatedContainers();
      logger.info("  " + retry + "  Allocate containers = " + containers.size() + ", retry = " + retry + ", duration = " + (retry * 500));
      for (Container container : containers) {
        if(selContainer == null) selContainer = container;
        //else amrmClient.releaseAssignedContainer(container.getId());
      }
      Thread.sleep(500);
    }
    if(selContainer != null) {
      vmReq.getCallback().onAllocate(this, vmReq, selContainer);
    }
    amrmClient.removeContainerRequest(containerReq);
    return selContainer ;
  }
}