package com.neverwinterdp.scribengin.dataflow.chain;

import java.util.List;

import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;

public class DataflowChainConfig {
  static public enum Submitter {Parallel, Order} ;

  private Submitter submitter = Submitter.Order ;
  private List<DataflowDescriptor> descriptors ;

  public Submitter getSubmitter() { return submitter; }
  public void setSubmitter(Submitter submitter) { this.submitter = submitter; }
  
  public List<DataflowDescriptor> getDescriptors() { return descriptors; }
  public void setDescriptors(List<DataflowDescriptor> descriptors) { this.descriptors = descriptors; }
}
