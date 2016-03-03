package com.neverwinterdp.scribengin.dataflow;

import java.util.LinkedHashMap;
import java.util.Map;

import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.es.ESStorageConfig;
import com.neverwinterdp.storage.hdfs.HDFSStorageConfig;
import com.neverwinterdp.storage.kafka.KafkaStorageConfig;
import com.neverwinterdp.storage.nulldev.NullDevStorageConfig;

public class Dataflow {
  
  private Map<String, DataSet<?>>     dataSets           = new LinkedHashMap<>();
  private Map<String, Operator<?, ?>> operators          = new LinkedHashMap<>();
  private WireDataSetFactory          wireDataStreamFactory;
  private DataflowDescriptor          dataflowDescriptor = new DataflowDescriptor();
  
  public Dataflow(String id) {
    dataflowDescriptor = new DataflowDescriptor(id, id);
  }
  
  public String getDataflowId() { return dataflowDescriptor.getId(); }
  
  public MasterDescriptor getMasterDescriptor() { return dataflowDescriptor.getMaster() ; }
  
  public WorkerDescriptor getWorkerDescriptor() { return dataflowDescriptor.getWorker(); }
  
  public Dataflow setDFSAppHome(String dfsAppHome) {
    dataflowDescriptor.setDataflowAppHome(dfsAppHome);
    return this;
  }
  
  public Dataflow useWireDataSetFactory(WireDataSetFactory factory) {
    wireDataStreamFactory = factory;
    return this;
  }
  
  public Dataflow setMaxRuntime(long maxRuntime) {
    dataflowDescriptor.setMaxRunTime(maxRuntime);;
    return this;
  }
  
  public Dataflow setTrackingWindowSize(int size) {
    dataflowDescriptor.setTrackingWindowSize(size);;
    return this;
  }
  
  public Dataflow setSlidingWindowSize(int size) {
    dataflowDescriptor.setSlidingWindowSize(size);;
    return this;
  }
  
  public Dataflow setDefaultParallelism(int parallelism) {
    dataflowDescriptor.getStreamConfig().setParallelism(parallelism);
    return this;
  }
  
  public Dataflow setDefaultReplication(int replication) {
    dataflowDescriptor.getStreamConfig().setReplication(replication);
    return this;
  }
  
  public <IN> KafkaDataSet<IN> createInput(KafkaStorageConfig config) {
    KafkaDataSet<IN> ds = new KafkaDataSet<IN>(DataStreamType.Input, config);
    dataSets.put(ds.getName(), ds);
    return ds;
  }

  public <OUT> KafkaDataSet<OUT> createOutput(KafkaStorageConfig config) {
    KafkaDataSet<OUT> ds = new KafkaDataSet<OUT>(DataStreamType.Output, config);
    dataSets.put(ds.getName(), ds);
    return ds;
  }
  
  public <OUT> NullDevDataSet<OUT> createOutput(NullDevStorageConfig config) {
    NullDevDataSet<OUT> ds = new NullDevDataSet<OUT>(DataStreamType.Output, config);
    dataSets.put(ds.getName(), ds);
    return ds;
  }
  
  public <OUT> HDFSDataSet<OUT> createOutput(HDFSStorageConfig config) {
    HDFSDataSet<OUT> ds = new HDFSDataSet<OUT>(DataStreamType.Output, config);
    dataSets.put(ds.getName(), ds);
    return ds;
  }
  
  public <OUT> ESDataSet<OUT> createOutput(ESStorageConfig config){
    ESDataSet<OUT> ds = new ESDataSet<OUT>(DataStreamType.Output, config);
    dataSets.put(ds.getName(), ds);
    return ds;
  }
  
  <T> DataSet<T> getOrCreateWireDataSet(String name) {
    DataSet<T> ds = getDataStream(name);
    if(ds != null) return ds;
    ds = wireDataStreamFactory.createDataStream(this, name);
    dataSets.put(name, ds);
    return ds;
  }
  
  public DataSet<?>[] getDataSets() {
    DataSet<?>[] array = new DataSet<?>[dataSets.size()];
    return dataSets.values().toArray(array);
  }
  
  public <I, O> Operator<I, O> createOperator(String name, Class<? extends DataStreamOperator> dataStreamOperator) {
    Operator<I, O> operator = new Operator<I, O>(this, name, dataStreamOperator);
    operator.add(MTDataStreamOperatorInterceptor.class);
    operators.put(name, operator);
    return operator;
  }
  
  @SuppressWarnings("unchecked")
  public <I, O> Operator<I, O>[] getOperators() {
    return operators.values().toArray(new Operator[operators.size()]);
  }
  
  @SuppressWarnings("unchecked")
  public <T> DataSet<T> getDataStream(String name) {
    return (DataSet<T>) dataSets.get(name);
  }
  
  <T> void checkValidDataStream(DataSet<T> ds) {
    if(ds != getDataStream(ds.getName())) {
      throw new RuntimeException("The data stream " + ds.getName() + " is not valid and belong to the dataflow " + getDataflowId());
    }
  }
  
  public DataflowDescriptor buildDataflowDescriptor() {
    DataflowDescriptor config = dataflowDescriptor;
    DataSet<?>[] dataStreams = getDataSets();
    config.getStreamConfig().clear();
    for(int i = 0; i < dataStreams.length; i++) {
      DataSet<?> sel = dataStreams[i];
      StorageConfig storageConfig = sel.getStorageConfig();
      if(sel.getDataStreamType() == DataStreamType.Output) {
        storageConfig.setPartitionStream(config.getStreamConfig().getParallelism());
      }
      config.getStreamConfig().add(sel.getName(), storageConfig);
    }
    Operator<?, ?>[] operators = getOperators();
    config.clearOperators();
    for(int i = 0; i < operators.length; i++) {
      config.addOperator(operators[i].getOperatorDescriptor());
    }
    return config ;
  }
}
