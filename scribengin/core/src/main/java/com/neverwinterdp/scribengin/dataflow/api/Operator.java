package com.neverwinterdp.scribengin.dataflow.api;

import java.util.HashSet;
import java.util.Set;

public class Operator<I, O> {
  private String                 name;
  private Dataflow<?, ?>         dataflow;
  private Set<String>            inputs  = new HashSet<String>();
  private Set<String>            outputs = new HashSet<String>();
  private OperatorFunction<I, O> operatorFunction;
  private OutputSelector<O>      outputSelector;

  Operator(Dataflow<?, ?> dataflow, String name) {
    this.dataflow = dataflow;
    this.name     = name;
  }
  
  Operator(Dataflow<?, ?> dataflow, String name, OperatorFunction<I, O> func, OutputSelector<O> selector) {
    this(dataflow, name);
    operatorFunction = func;
    outputSelector   = selector;
  }

  public String getName() { return name; }
  public void setName(String name) { this.name = name; }
  
  public Operator<I, O> connect(DataStream<O> out) {
    out(out);
    return this;
  }
  
  public <T> Operator<I, O> connect(Operator<O, T> nextOp) {
    DataStream<O> ds  = dataflow.getOrCreateWireDataStream(name + "-to-" + nextOp.getName());
    out(ds);
    nextOp.in(ds);
    return this;
  }
  
  public <T> Operator<I, O> with(OutputSelector<O> selector) {
    outputSelector = selector;
    return this;
  }
  
  Operator<I, O> in(DataStream<I> in) {
    dataflow.checkValidDataStream(in);
    if(inputs.contains(in.getName())) {
      throw new RuntimeException("The operator " + name + " is already connected to the input stream " + in.getName());
    }
    inputs.add(in.getName());
    return this;
  }

  Operator<I, O> out(DataStream<O> out) {
    dataflow.checkValidDataStream(out);
    if(outputs.contains(out.getName())) {
      throw new RuntimeException("The operator " + name + " is already connected to the output stream " + out.getName());
    }
    outputs.add(out.getName());
    return this;
  }

  public OperatorDescriptor getOperatorDescriptor() {
    OperatorDescriptor descriptor = new OperatorDescriptor();
    descriptor.setOperator(name);
    descriptor.setInputs(inputs);
    descriptor.setOutputs(outputs);
    return descriptor ;
  }
  
}
