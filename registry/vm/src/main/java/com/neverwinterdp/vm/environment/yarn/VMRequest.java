package com.neverwinterdp.vm.environment.yarn;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import com.neverwinterdp.vm.environment.yarn.YarnManager.ContainerRequestCallback;

public class VMRequest {
  static public AtomicLong idTracker = new AtomicLong() ;
 
  private long id ;
  private Resource capability;
  private String[] nodes;
  private String[] racks;
  private Priority priority;
  private ContainerRequest containerRequest;
  private ContainerRequestCallback callback ;
  
  public VMRequest(Resource capability, String[] nodes, String[] racks, Priority priority) {
    this.id = idTracker.getAndIncrement();
    this.capability = capability;
    this.nodes = nodes;
    this.racks = racks;
    this.priority = priority;
  }

  public long getId() { return id; }
  
  public Resource getCapability() { return this.capability; }
  
  public ContainerRequest getContainerRequest() {
    if(containerRequest == null) {
      containerRequest = new ContainerRequest(capability, nodes, racks, priority);
    }
    return containerRequest;
  }
  
  public void reset() {
    containerRequest = null;
  }
  
  public ContainerRequestCallback getCallback() { return callback; }
  public void setCallback(ContainerRequestCallback callback) {
    this.callback = callback;
  }
}