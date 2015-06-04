package com.neverwinterdp.scribengin.dataflow.chain;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowSubmitter;

abstract public class DataflowChainSubmitter {
  protected ScribenginClient client;
  protected String dataflowHome;
  protected DataflowChainConfig config;
  private List<DataflowSubmitter> submitters = new ArrayList<>();

  public DataflowChainSubmitter(ScribenginClient client, String dataflowHome, DataflowChainConfig config) {
    this.client = client;
    this.dataflowHome = dataflowHome ;
    this.config = config ;
  }
  
  public void submit(long timeout) throws Exception {
    long stopTime = System.currentTimeMillis() + timeout;
    long remainTime = timeout;
    for(DataflowDescriptor sel : config.getDescriptors()) {
      DataflowSubmitter submitter = doSubmit(client, dataflowHome, sel) ;
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
      submitter.waitForTerminated(remainTime);
      remainTime = stopTime - System.currentTimeMillis();
    }
  }
  
  abstract protected DataflowSubmitter doSubmit(ScribenginClient client, String dataflowHome, DataflowDescriptor descriptor) throws Exception;

}
