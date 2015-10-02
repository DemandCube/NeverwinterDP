package com.neverwinterdp.scribengin.dataflow.config;

import java.util.Set;

public class OperatorConfig {
  private String      scribe;
  private Set<String> inputs;
  private Set<String> outputs;
  
  public String getScribe() { return scribe; }
  public void   setScribe(String scribe) { this.scribe = scribe;}
  
  public Set<String> getInputs() { return inputs; }
  public void setInputs(Set<String> inputs) { this.inputs = inputs; }
  
  public Set<String> getOutputs() { return outputs; }
  public void        setOutputs(Set<String> outputs) { this.outputs = outputs; }
}
