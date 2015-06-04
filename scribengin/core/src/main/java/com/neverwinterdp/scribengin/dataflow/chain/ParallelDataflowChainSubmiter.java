package com.neverwinterdp.scribengin.dataflow.chain;

import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowSubmitter;

public class ParallelDataflowChainSubmiter extends DataflowChainSubmitter {

  public ParallelDataflowChainSubmiter(ScribenginClient client, String dataflowHome, DataflowChainConfig config) {
    super(client, dataflowHome, config) ;
  }

  @Override
  protected DataflowSubmitter doSubmit(ScribenginClient client, String dataflowHome, DataflowDescriptor descriptor) throws Exception {
    return null;
  }
  
}
