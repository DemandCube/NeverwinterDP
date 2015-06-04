package com.neverwinterdp.scribengin.dataflow.chain;

import com.neverwinterdp.scribengin.ScribenginClient;

public class ParallelDataflowChainSubmiter extends DataflowChainSubmitter {

  public ParallelDataflowChainSubmiter(ScribenginClient scribenginClient) {
    super(scribenginClient) ;
  }
  
  public void submit(String dataflowHome, DataflowChainConfig config, long timeout) throws Exception {
  }
}
