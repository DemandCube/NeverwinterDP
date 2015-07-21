package com.neverwinterdp.scribengin.scribe;

import com.neverwinterdp.scribengin.dataflow.DataflowInstruction;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskContext;
import com.neverwinterdp.scribengin.dataflow.DataflowMessage;

public abstract class ScribeAbstract {
  protected ScribeState state;
  
  abstract public void process(DataflowMessage dataflowMessage, DataflowTaskContext ctx) throws Exception;
  
  public void process(DataflowInstruction instruction, DataflowTaskContext ctx) throws Exception {
  }
  
  public ScribeState getState() { return this.state; }
  
  public void setState(ScribeState newState) { this.state = newState; }
}