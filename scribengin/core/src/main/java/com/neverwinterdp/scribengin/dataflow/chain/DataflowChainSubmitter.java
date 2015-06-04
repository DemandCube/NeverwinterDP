package com.neverwinterdp.scribengin.dataflow.chain;

import com.neverwinterdp.scribengin.ScribenginClient;

abstract public class DataflowChainSubmitter {
  protected ScribenginClient scribenginClient;

  public DataflowChainSubmitter(ScribenginClient scribenginClient) {
    this.scribenginClient = scribenginClient;
  }
  
  abstract public void submit(String dataflowHome, DataflowChainConfig config, long timeout) throws Exception ;
}
