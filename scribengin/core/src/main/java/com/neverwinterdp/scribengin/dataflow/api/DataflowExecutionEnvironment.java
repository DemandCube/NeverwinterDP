package com.neverwinterdp.scribengin.dataflow.api;

public class DataflowExecutionEnvironment {
  
  public void submit(Dataflow<?, ?> dataflow) throws Exception {
  }
  
  public DataflowDescriptor build(Dataflow<?, ?> dataflow) {
    DataflowDescriptor config = new DataflowDescriptor();
    config.setId(dataflow.getDataflowId());
    DataStream<?>[] dataStreams = dataflow.getDataStreams();
    for(int i = 0; i < dataStreams.length; i++) {
      DataStream<?> sel = dataStreams[i];
      config.getStreamConfig().add(sel.getName(), sel.getStorageConfig());
    }
    config.getStreamConfig();
    return config ;
  }
}
