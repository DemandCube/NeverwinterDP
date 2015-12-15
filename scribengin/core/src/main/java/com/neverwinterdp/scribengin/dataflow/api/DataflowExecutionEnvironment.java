package com.neverwinterdp.scribengin.dataflow.api;

import com.neverwinterdp.scribengin.dataflow.config.DataflowConfig;

public class DataflowExecutionEnvironment {
  
  public void submit(Dataflow<?, ?> dataflow) throws Exception {
  }
  
  public DataflowConfig build(Dataflow<?, ?> dataflow) {
    DataflowConfig config = new DataflowConfig();
    config.setId(dataflow.getDataflowId());
    return config ;
  }
  
}
