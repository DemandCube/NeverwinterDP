package com.neverwinterdp.scribengin.dataflow;

import java.util.Set;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.registry.task.TaskExecutorDescriptor;
import com.neverwinterdp.scribengin.dataflow.runtime.DataStreamOperatorDescriptor;
import com.neverwinterdp.scribengin.dataflow.runtime.DataStreamOperatorReport;
import com.neverwinterdp.vm.VMDescriptor;

public interface DataStreamOperatorContext {
  public String getId();
  
  public DataStreamOperatorDescriptor getDescriptor() ;
  
  public DataStreamOperatorReport getReport() ;

  public TaskExecutorDescriptor getTaskExecutor();
  
  public VMDescriptor getVM() ;
  
  public <T> T getService(Class<T> type);
  
  public boolean isComplete() ;
  
  public void setComplete() ;
  
  public Set<String> getAvailableOutputs() ;
  
  public void write(String name, Message record) throws Exception ;
  
  public void commit() throws Exception ;
  
  public void rollback() throws Exception ;

  public void close() throws Exception ;
  
}
