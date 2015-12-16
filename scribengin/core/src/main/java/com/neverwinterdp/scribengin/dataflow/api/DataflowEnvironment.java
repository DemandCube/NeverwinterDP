package com.neverwinterdp.scribengin.dataflow.api;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.scribengin.dataflow.DataflowSubmitter;

public class DataflowEnvironment {
  private Registry registry;
  
  protected DataflowEnvironment() { }
  
  public DataflowEnvironment(Registry registry) { 
    init(registry);
  }
  
  public DataflowEnvironment(RegistryConfig config) throws Exception { 
    init(config.newInstance());
  }
  
  protected void init(Registry registry) {
    this.registry = registry;
  }
  
  public void submit(Dataflow<?, ?> dataflow, long maxWaitForRunning) throws Exception {
    DataflowSubmitter submitter = new DataflowSubmitter(registry, dataflow.buildDataflowDescriptor());
    submitter.submit();
    if(maxWaitForRunning > 0) {
      submitter.waitForRunning(maxWaitForRunning);
    }
  }
}
