package com.neverwinterdp.scribengin.dataflow.api;

import java.util.LinkedHashMap;
import java.util.Map;

import com.neverwinterdp.storage.kafka.KafkaStorageConfig;

public class Dataflow<IN, OUT> {
  
  private String                       dataflowId;
  private Map<String, DataStream<?>>   dataStreams  = new LinkedHashMap<>();
  private Map<String, Operator<?, ?>>  operators    = new LinkedHashMap<>();
  private WireDataStreamFactory        wireDataStreamFactory;
  
  public Dataflow(String id) {
    this.dataflowId = id;
  }
  
  public String getDataflowId() { return this.dataflowId; }
  
  public Dataflow<IN, OUT> useWireDataStreamFactory(WireDataStreamFactory factory) {
    wireDataStreamFactory = factory;
    return this;
  }
  
  public KafkaDataStream<IN> createInput(KafkaStorageConfig config) {
    KafkaDataStream<IN> ds = new KafkaDataStream<IN>(DataStreamType.Input, config);
    dataStreams.put(ds.getName(), ds);
    return ds;
  }
  
  public KafkaDataStream<OUT> createOutput(KafkaStorageConfig config) {
    KafkaDataStream<OUT> ds = new KafkaDataStream<OUT>(DataStreamType.Output, config);
    dataStreams.put(ds.getName(), ds);
    return ds;
  }
  
  <T> DataStream<T> getOrCreateWireDataStream(String name) {
    DataStream<T> ds = getDataStream(name);
    if(ds != null) return ds;
    ds = wireDataStreamFactory.createDataStream(this, name);
    dataStreams.put(name, ds);
    return ds;
  }
  
  public DataStream<?>[] getDataStreams() {
    DataStream<?>[] array = new DataStream<?>[dataStreams.size()];
    return dataStreams.values().toArray(array);
  }
  
  public <I, O> Operator<I, O> createOperator(String name) {
    Operator<I, O> operator = new Operator<I, O>(this, name);
    operators.put(name, operator);
    return operator;
  }
  
  @SuppressWarnings("unchecked")
  public <I, O> Operator<I, O>[] getOperators() {
    return operators.values().toArray(new Operator[operators.size()]);
  }
  
  @SuppressWarnings("unchecked")
  public <T> DataStream<T> getDataStream(String name) {
    return (DataStream<T>) dataStreams.get(name);
  }
  
  <T> void checkValidDataStream(DataStream<T> ds) {
    if(ds != getDataStream(ds.getName())) {
      throw new RuntimeException("The data stream " + ds.getName() + " is not valid and belong to the dataflow " + dataflowId);
    }
  }
  
  public DataflowDescriptor buildDataflowDescriptor() {
    DataflowDescriptor config = new DataflowDescriptor(dataflowId, dataflowId);
    DataStream<?>[] dataStreams = getDataStreams();
    for(int i = 0; i < dataStreams.length; i++) {
      DataStream<?> sel = dataStreams[i];
      config.getStreamConfig().add(sel.getName(), sel.getStorageConfig());
    }
    Operator<?, ?>[] operators = getOperators();
    for(int i = 0; i < operators.length; i++) {
      config.addOperator(operators[i].getOperatorDescriptor());
    }
    return config ;
  }
}
