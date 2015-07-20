package com.neverwinterdp.scribengin.dataflow.chain;

import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowSubmitter;

public class OrderDataflowChainSubmitter extends DataflowChainSubmitter {
  
  public OrderDataflowChainSubmitter(ScribenginClient client, String dfsDataflowHome, DataflowChainConfig config) {
    super(client, dfsDataflowHome, config);
  }
  
  public void submit(long timeout) throws Exception {
    long stopTime = System.currentTimeMillis() + timeout;
    long remainTime = timeout;
    for(int i = 0; i < config.getDescriptors().size(); i++) {
      DataflowDescriptor sel = config.getDescriptors().get(i);
      remainTime = stopTime - System.currentTimeMillis() ;
      DataflowSubmitter submitter = doSubmit(client, dfsDataflowHome, sel, remainTime) ;
      submitters.add(submitter);
      remainTime = stopTime - System.currentTimeMillis();
      if(remainTime < 0) {
        throw new InterruptedException("Cannot finish to submit within " + timeout + "ms") ;
      }
      //if(i == 0) Thread.sleep(25000);
    }
  }
  
  protected DataflowSubmitter doSubmit(ScribenginClient client, String dfsDataflowHome, DataflowDescriptor descriptor, long timeout) throws Exception {
    DataflowSubmitter submitter = new DataflowSubmitter(client, dfsDataflowHome, descriptor) ;
    submitter.submit();
    setupDebugger(submitter);
    submitter.waitForRunning(timeout);
    if(descriptor.getId().indexOf("splitter") > 0) {
      Thread.sleep(60000);
    }
    return submitter;
  }
  
  public void waitForTerminated(long timeout) throws Exception {
    long stopTime = System.currentTimeMillis() + timeout;
    long remainTime = timeout;
    for(DataflowSubmitter submitter : submitters) {
      submitter.waitForFinish(remainTime);
      remainTime = stopTime - System.currentTimeMillis();
    }
  }
}