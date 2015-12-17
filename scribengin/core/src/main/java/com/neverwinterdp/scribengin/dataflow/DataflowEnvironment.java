package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.scribengin.ScribenginClient;

public class DataflowEnvironment {
  private Registry registry;
  private ScribenginClient scribenginClient;
  
  public DataflowEnvironment(RegistryConfig config) throws Exception { 
    this(config.newInstance());
  }
  
  public DataflowEnvironment(Registry registry) { 
    this.scribenginClient = new ScribenginClient(registry);
  }
  
  public DataflowEnvironment(ScribenginClient scribenginClient) { 
    this.scribenginClient = new ScribenginClient(registry);
  }
  
  public void submit(Dataflow<?, ?> dataflow, long maxWaitForRunning) throws Exception {
    DataflowSubmitter submitter = new DataflowSubmitter(registry, dataflow.buildDataflowDescriptor());
    submitter.submit();
    if(maxWaitForRunning > 0) {
      submitter.waitForRunning(maxWaitForRunning);
    }
  }
}
