package com.neverwinterdp.scribengin.dataflow;

import java.util.Set;

import com.neverwinterdp.scribengin.dataflow.runtime.DataStreamOperatorDescriptor;
import com.neverwinterdp.scribengin.dataflow.runtime.DataStreamOperatorReport;
import com.neverwinterdp.storage.Record;

public interface DataStreamOperatorContext {
  
  public DataStreamOperatorDescriptor getDescriptor() ;
  
  public DataStreamOperatorReport getReport() ;
  
  public boolean isComplete() ;
  
  public void setComplete() ;
  
  public Set<String> getAvailableOutputs() ;
  
  public void write(String name, Record record) throws Exception ;
  
  public void commit() throws Exception ;
  
  public void rollback() throws Exception ;

  public void close() throws Exception ;
}
