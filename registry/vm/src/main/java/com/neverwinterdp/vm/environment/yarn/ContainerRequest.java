package com.neverwinterdp.vm.environment.yarn;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;

import com.neverwinterdp.vm.environment.yarn.YarnManager.ContainerRequestCallback;

public class ContainerRequest extends org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest {
  static public AtomicLong idTracker = new AtomicLong() ;
  private long id ;
  private ContainerRequestCallback callback ;
  
  public ContainerRequest(Resource capability, String[] nodes, String[] racks, Priority priority) {
    super(capability, nodes, racks, priority);
    id = idTracker.getAndIncrement();
  }

  public long getId() { return id; }

  public ContainerRequestCallback getCallback() { return callback; }
  public void setCallback(ContainerRequestCallback callback) {
    this.callback = callback;
  }
}