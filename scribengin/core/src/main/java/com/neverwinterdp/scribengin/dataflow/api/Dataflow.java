package com.neverwinterdp.scribengin.dataflow.api;

import java.util.LinkedHashMap;
import java.util.Map;

public class Dataflow<IN, OUT> {
  private String                       dataflowId;
  private Map<String, DataStream<IN>>  inDataStreams  = new LinkedHashMap<>();
  private Map<String, DataStream<OUT>> outDataStreams = new LinkedHashMap<>();
  
  
  public Dataflow(String id) {
    this.dataflowId = id;
  }
  
  public String getDataflowId() { return this.dataflowId; }
  
  public DataStream<IN> addInput(DataStream<IN> ds) {
    inDataStreams.put(ds.getName(), ds);
    return ds;
  }
  
  public DataStream<OUT> addOutput(DataStream<OUT> ds) {
    outDataStreams.put(ds.getName(), ds);
    return ds;
  }
}
