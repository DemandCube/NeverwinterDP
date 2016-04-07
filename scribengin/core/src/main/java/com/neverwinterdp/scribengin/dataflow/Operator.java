package com.neverwinterdp.scribengin.dataflow;

import java.util.HashSet;
import java.util.Set;

public class Operator {
  private String                              name;
  private Dataflow                            dataflow;
  private Class<? extends DataStreamOperator> dataStreamOperator;
  private Set<String>                         interceptors = new HashSet<>();
  private Set<String>                         inputs       = new HashSet<>();
  private Set<String>                         outputs      = new HashSet<>();

  Operator(Dataflow dataflow, String name, Class<? extends DataStreamOperator> dataStreamOperator) {
    this.dataflow           = dataflow;
    this.name               = name;
    this.dataStreamOperator = dataStreamOperator;
  }
  
  public String getName() { return name; }
  public void setName(String name) { this.name = name; }
  
  public Operator add(Class<? extends DataStreamOperatorInterceptor> type) {
    interceptors.add(type.getName());
    return this;
  }
  
  public Operator connect(DataSet<?> out) {
    out(out);
    return this;
  }
  
  public Operator connect(Operator nextOp) {
    DataSet<?> ds  = dataflow.getOrCreateWireDataSet(name + "-to-" + nextOp.getName());
    out(ds);
    nextOp.in(ds);
    return this;
  }
  
  Operator in(DataSet<?> in) {
    dataflow.checkValidDataStream(in);
    if(inputs.contains(in.getName())) {
      throw new RuntimeException("The operator " + name + " is already connected to the input stream " + in.getName());
    }
    inputs.add(in.getName());
    return this;
  }

  Operator out(DataSet<?> out) {
    dataflow.checkValidDataStream(out);
    if(outputs.contains(out.getName())) {
      throw new RuntimeException("The operator " + name + " is already connected to the output stream " + out.getName());
    }
    outputs.add(out.getName());
    return this;
  }

  public OperatorDescriptor getOperatorDescriptor() {
    OperatorDescriptor descriptor = new OperatorDescriptor();
    descriptor.setName(name);
    descriptor.setOperator(dataStreamOperator.getName());
    descriptor.setInterceptors(interceptors);
    descriptor.setInputs(inputs);
    descriptor.setOutputs(outputs);
    return descriptor ;
  }
}
