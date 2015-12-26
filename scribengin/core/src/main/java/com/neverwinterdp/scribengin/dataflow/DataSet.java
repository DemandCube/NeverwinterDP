package com.neverwinterdp.scribengin.dataflow;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.neverwinterdp.storage.StorageConfig;

abstract public class DataSet<T> {
  final static public String DATAFLOW_SOURCE_INPUT        = "dataflow.source.input";
  final static public String DATAFLOW_SOURCE_INTERCEPTORS = "dataflow.source.interceptors";
  
  final static public String DATAFLOW_SINK_INTERCEPTORS   = "dataflow.sink.interceptors";
  
  private String name;
  private DataSetType   type = DataSetType.Wire;
  private Map<String, Class<?>> sourceInterceptors = new HashMap<>();
  private Map<String, Class<?>> sinkInterceptors = new HashMap<>();
  
  public DataSet(String name, DataSetType type) {
    this.name = name;
    this.type = type ;
    if(type == DataSetType.Input) {
      addSourceInterceptor("input", MTInputDataStreamInterceptor.class);
    }
    if(type == DataSetType.Output) {
      addSinkInterceptor("output", MTOutputDataStreamInterceptor.class);
    }
  }
  
  public String getName() { return this.name; }
  
  public <OUT> DataSet<T> connect(Operator<T, OUT> operator) {
    operator.in(this);
    return this;
  }
  
  public DataSet<T> addSourceInterceptor(String name, Class<? extends DataStreamSourceInterceptor> interceptor) {
    if(type != DataSetType.Input) {
      throw new RuntimeException("Can add the source interceptor to the input only");
    }
    sourceInterceptors.put(name, interceptor);
    return this;
  }
  
  public DataSet<T> addSinkInterceptor(String name, Class<? extends DataStreamSinkInterceptor> interceptor) {
    if(type != DataSetType.Output) {
      throw new RuntimeException("Can add the sink interceptor to the output only");
    }
    sinkInterceptors.put(name, interceptor);
    return this;
  }
  
  abstract protected StorageConfig createStorageConfig() ;
  
  public StorageConfig getStorageConfig() {
    StorageConfig storageConfig = createStorageConfig() ;
    if(sourceInterceptors.size() > 0) {
      storageConfig.attribute(DATAFLOW_SOURCE_INTERCEPTORS, joinClassTypes(sourceInterceptors.values()));
    }

    if(type == DataSetType.Input) {
      storageConfig.attribute(DATAFLOW_SOURCE_INPUT, true);
    }
    if(sinkInterceptors.size() > 0) {
      storageConfig.attribute(DATAFLOW_SINK_INTERCEPTORS,joinClassTypes(sinkInterceptors.values()));
    }
    return storageConfig;
  }
  
  String joinClassTypes(Collection<Class<?>> types) {
    StringBuilder b = new StringBuilder();
    for(Class<?> sel : types) {
      if(b.length() > 0) b.append(",");
      b.append(sel.getName());
    }
    return b.toString();
  }
}
