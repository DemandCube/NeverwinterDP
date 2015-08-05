package com.neverwinterdp.scribengin.dataflow.worker;

import com.neverwinterdp.registry.RegistryException;

abstract public class DataflowTaskExecutor {
  protected DataflowTaskExecutorDescriptor executorDescriptor;
  protected DataflowTaskExecutorService    executorService;
  
  public DataflowTaskExecutor(DataflowTaskExecutorService service,
                              DataflowTaskExecutorDescriptor descriptor) throws RegistryException {
    executorDescriptor = descriptor;
    this.executorService = service;
    service.
      getDataflowRegistry().
      getWorkerRegistry().
      createWorkerTaskExecutor(service.getVMDescriptor(), descriptor);
  }

  protected void notifyExecutorTermination() {
    executorService.getDataflowTaskExecutors().notifyExecutorTermination();
  }
  
  abstract public void start() ;
  
  abstract public void interrupt() throws Exception ;
  
  abstract public boolean isAlive() ;
  
  abstract public void simulateKill() throws Exception ;
}