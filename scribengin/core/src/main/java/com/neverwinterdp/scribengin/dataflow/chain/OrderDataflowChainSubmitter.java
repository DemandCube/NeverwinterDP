package com.neverwinterdp.scribengin.dataflow.chain;

import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowSubmitter;

public class OrderDataflowChainSubmitter extends DataflowChainSubmitter {

  public OrderDataflowChainSubmitter(ScribenginClient scribenginClient) {
    super(scribenginClient);
  }
  
  public void submit(String dataflowHome, DataflowChainConfig config, long timeout) throws Exception {
    long stopTime = System.currentTimeMillis() + timeout;
    long remainTime = timeout ;
    for(DataflowDescriptor sel : config.getDescriptors()) {
      DataflowSubmitter submitter = new DataflowSubmitter(scribenginClient, dataflowHome, sel) ;
      submitter.submitAndWaitForRunningStatus(remainTime);
      remainTime = stopTime - System.currentTimeMillis();
      if(remainTime < 0) {
        throw new InterruptedException("Cannot finish to submit within " + timeout + "ms") ;
      }
    }
  }
}