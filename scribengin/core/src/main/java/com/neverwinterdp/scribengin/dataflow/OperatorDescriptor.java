package com.neverwinterdp.scribengin.dataflow;

import java.util.Set;

public class OperatorDescriptor {
  private String      name ;
  private String      operator;
  
  private Set<String> inputs;
  private Set<String> outputs;
  
  public String getName() { return name; }
  public void setName(String name) { this.name = name; }

  public String getOperator() { return operator; }
  public void   setOperator(String operator) { this.operator = operator;}
  
  public Set<String> getInputs() { return inputs; }
  public void        setInputs(Set<String> inputs) { this.inputs = inputs; }
  
  public Set<String> getOutputs() { return outputs; }
  public void        setOutputs(Set<String> outputs) { this.outputs = outputs; }
}
