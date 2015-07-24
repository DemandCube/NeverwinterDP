package com.neverwinterdp.scribengin.dataflow.chain;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowSubmitter;
import com.neverwinterdp.yara.snapshot.ClusterMetricRegistrySnapshot;

abstract public class DataflowChainSubmitter {
  protected ScribenginClient client;
  protected String dfsDataflowHome;
  protected DataflowChainConfig config;
  protected List<DataflowSubmitter> submitters = new ArrayList<>();
  private boolean enableDataflowTaskDebugger = false;

  public DataflowChainSubmitter(ScribenginClient client, String dfsDataflowHome, DataflowChainConfig config) {
    this.client = client;
    this.dfsDataflowHome = dfsDataflowHome ;
    this.config = config ;
    
    
  }
  
  public List<DataflowSubmitter> getDataflowSubmitters() { return this.submitters ; }
  
  public void submit(long timeout) throws Exception {
    long stopTime = System.currentTimeMillis() + timeout;
    long remainTime = timeout;
    for(DataflowDescriptor sel : config.getDescriptors()) {
      remainTime = stopTime - System.currentTimeMillis();
      DataflowSubmitter submitter = doSubmit(client, dfsDataflowHome, sel, remainTime) ;
      submitters.add(submitter);
      remainTime = stopTime - System.currentTimeMillis();
      if(remainTime < 0) {
        throw new InterruptedException("Cannot finish to submit within " + timeout + "ms") ;
      }
    }
  }
  
  public void waitForTerminated(long timeout) throws Exception {
    long stopTime = System.currentTimeMillis() + timeout;
    long remainTime = timeout;
    for(DataflowSubmitter submitter : submitters) {
      submitter.waitForFinish(remainTime);
      remainTime = stopTime - System.currentTimeMillis();
    }
  }
  
  public DataflowChainSubmitter enableDataflowTaskDebugger() throws Exception {
    enableDataflowTaskDebugger = true;
    return this ;
  }
  
  public void report(Appendable out) throws Exception {
    for(DataflowSubmitter submitter : submitters) {
      submitter.report(out);
    }
  }
  
  protected void setupDebugger(DataflowSubmitter submitter) throws Exception {
    if(enableDataflowTaskDebugger) {
      submitter.enableDataflowTaskDebugger(System.out);
    }
  }
  
  public List<ClusterMetricRegistrySnapshot> getMetrics() throws RegistryException {
    List<ClusterMetricRegistrySnapshot> holder = new ArrayList<>();
    for(DataflowSubmitter sel : submitters) {
      ClusterMetricRegistrySnapshot dflMetrics = sel.getDataflowMetrics() ;
      holder.add(dflMetrics);
    }
    return holder;
  }
  
  abstract protected DataflowSubmitter doSubmit(ScribenginClient client, String dfsDataflowHome, DataflowDescriptor descriptor, long timeout) throws Exception;

}
