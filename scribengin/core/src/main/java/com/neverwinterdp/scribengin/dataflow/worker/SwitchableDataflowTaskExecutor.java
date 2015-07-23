package com.neverwinterdp.scribengin.dataflow.worker;

import com.neverwinterdp.registry.RegistryException;

public class SwitchableDataflowTaskExecutor extends DataflowTaskExecutor {

  public SwitchableDataflowTaskExecutor(DataflowTaskExecutorService service, DataflowTaskExecutorDescriptor descriptor) throws RegistryException {
    super(service, descriptor);
  }
  
}