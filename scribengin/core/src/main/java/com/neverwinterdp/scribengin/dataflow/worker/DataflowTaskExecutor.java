package com.neverwinterdp.scribengin.dataflow.worker;

import com.neverwinterdp.registry.RegistryException;

abstract public class DataflowTaskExecutor {
  protected DataflowTaskExecutorDescriptor executorDescriptor;
  protected DataflowTaskExecutorService    executorService;
  

  public DataflowTaskExecutor(DataflowTaskExecutorService service, DataflowTaskExecutorDescriptor descriptor) throws RegistryException {
    executorDescriptor = descriptor;
    this.executorService = service;
    service.getDataflowRegistry().createWorkerTaskExecutor(service.getVMDescriptor(), descriptor);
  }
  
  abstract public void start() ;
  
  abstract public void interrupt() throws Exception ;
  
  abstract public boolean isAlive() ;
  
  abstract public void simulateKill() throws Exception ;
}