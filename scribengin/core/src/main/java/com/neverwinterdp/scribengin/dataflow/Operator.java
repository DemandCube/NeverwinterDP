package com.neverwinterdp.scribengin.dataflow;

import java.util.HashSet;
import java.util.Set;

public class Operator<I, O> {
  private String                              name;
  private Dataflow<?, ?>                      dataflow;
  private Class<? extends DataStreamOperator> dataStreamOperator;
  private Set<String>                         inputs  = new HashSet<String>();
  private Set<String>                         outputs = new HashSet<String>();

  Operator(Dataflow<?, ?> dataflow, String name, Class<? extends DataStreamOperator> dataStreamOperator) {
    this.dataflow           = dataflow;
    this.name               = name;
    this.dataStreamOperator = dataStreamOperator;
  }
  
  public String getName() { return name; }
  public void setName(String name) { this.name = name; }
  
  public Operator<I, O> connect(DataSet<O> out) {
    out(out);
    return this;
  }
  
  public <T> Operator<I, O> connect(Operator<O, T> nextOp) {
    DataSet<O> ds  = dataflow.getOrCreateWireDataSet(name + "-to-" + nextOp.getName());
    out(ds);
    nextOp.in(ds);
    return this;
  }
  
  Operator<I, O> in(DataSet<I> in) {
    dataflow.checkValidDataStream(in);
    if(inputs.contains(in.getName())) {
      throw new RuntimeException("The operator " + name + " is already connected to the input stream " + in.getName());
    }
    inputs.add(in.getName());
    return this;
  }

  Operator<I, O> out(DataSet<O> out) {
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
    descriptor.setInputs(inputs);
    descriptor.setOutputs(outputs);
    return descriptor ;
  }
  
}
