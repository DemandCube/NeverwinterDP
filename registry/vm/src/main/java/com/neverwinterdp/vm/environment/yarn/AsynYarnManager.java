package com.neverwinterdp.vm.environment.yarn;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mycila.jmx.annotation.JmxBean;
import com.neverwinterdp.vm.VMConfig;

@Singleton
@JmxBean("role=vm-manager, type=YarnManager, name=AsynYarnManager")
public class AsynYarnManager extends YarnManager {
  private AMRMClient<ContainerRequest> amrmClient ;
  private AMRMClientAsync<ContainerRequest> amrmClientAsync ;
  
  private ContainerRequestQueue containerRequestQueue = new ContainerRequestQueue ();
  private AtomicInteger countContainerRequest = new AtomicInteger();
  
  
  public AMRMClient<ContainerRequest> getAMRMClient() { return this.amrmClient ; }
  
  
  @Inject
  public void onInit(VMConfig vmConfig) throws Exception {
    logger.info("Start init(VMConfig vmConfig)");
    try {
      super.onInit(vmConfig);
      amrmClient = AMRMClient.createAMRMClient();
      amrmClientAsync = AMRMClientAsync.createAMRMClientAsync(amrmClient, 1000, new AMRMCallbackHandler());
      amrmClientAsync.init(conf);
      amrmClientAsync.start();
      // Register with RM
      String appHostName = InetAddress.getLocalHost().getHostAddress()  ;
      RegisterApplicationMasterResponse registerResponse = amrmClientAsync.registerApplicationMaster(appHostName, 0, "");
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
      if(amrmClientAsync != null) {
        amrmClientAsync.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
        amrmClientAsync.stop();
        amrmClientAsync.close(); 
      }
      super.onDestroy();
    } catch(Exception ex) {
      logger.error("Cannot destroy YarnManager properly", ex);
    }
    logger.info("Finish onDestroy()");
  }
  
  synchronized public void add(VMRequest vmReq, ContainerRequestCallback callback) {
    logger.info("Start add count = " + countContainerRequest.incrementAndGet());
    vmReq.setCallback(callback);
    containerRequestQueue.offer(vmReq);
    amrmClientAsync.addContainerRequest(vmReq.getContainerRequest());
    logger.info("Finish add");
  }
  
  class AMRMCallbackHandler implements AMRMClientAsync.CallbackHandler {
    private AtomicInteger countContainerAllocate = new AtomicInteger();
    private AtomicInteger countContainerComplete = new AtomicInteger();
    
    public void onContainersCompleted(List<ContainerStatus> statuses) {
      logger.info("Start onContainersCompleted count = " + countContainerComplete.incrementAndGet());
      for (ContainerStatus status: statuses) {
        assert (status.getState() == ContainerState.COMPLETE);
        int exitStatus = status.getExitStatus();
        //TODO: update vm descriptor status
        if (exitStatus != ContainerExitStatus.SUCCESS) {
        } else {
        }
      }
      logger.info("Finish onContainersCompleted(List<ContainerStatus> statuses)");
    }

    public void onContainersAllocated(List<Container> containers) {
      int numOfContainer = containers.size();
      logger.info("Start onContainersAllocated containers = " + numOfContainer);
      for (int i = 0; i < containers.size(); i++) {
        //TODO: review on allocated container code
        Container container = containers.get(i) ;
        VMRequest containerReq = containerRequestQueue.take(container);
        if(containerReq ==null) {
          logger.info("  onContainersAllocated: ignore container " + i);
          //TODO: research on this issue
          //http://hadoop.apache.org/docs/r2.6.0/api/org/apache/hadoop/yarn/client/api/AMRMClient.html#removeContainerRequest(T)
          continue;
        }
        containerReq.getCallback().onAllocate(AsynYarnManager.this, containerReq, container);
        amrmClientAsync.removeContainerRequest(containerReq.getContainerRequest());
        logger.info("  onContainersAllocated count = " + countContainerAllocate.incrementAndGet());
      }
      logger.info("Finish onContainersAllocated");
    }


    public void onNodesUpdated(List<NodeReport> updated) {
    }

    public void onError(Throwable e) {
      amrmClientAsync.stop();
    }

    public void onShutdownRequest() {
      //TODO: handle shutdown request
    }

    
    public float getProgress() { return 0; }
  }
  
  class ContainerRequestQueue {
    private List<VMRequest> queues = new ArrayList<>();
    
    synchronized public void offer(VMRequest request) {
      queues.add(request);
    }
    synchronized public VMRequest take(Container container) {
      int cpuCores = container.getResource().getVirtualCores();
      int memory = container.getResource().getMemory();
      for(int i = 0; i < queues.size(); i++) {
        VMRequest sel = queues.get(i);
        if(cpuCores == sel.getCapability().getVirtualCores() && 
            memory  == sel.getCapability().getMemory()) {
          queues.remove(sel);
          return sel;
        }
      }
      if(queues.size() > 0) {
        VMRequest sel = queues.get(0);
        queues.remove(sel);
        return sel;
      }
      return null;
    }
  }
}