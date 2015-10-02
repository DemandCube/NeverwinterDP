package com.neverwinterdp.scribengin.dataflow.registry;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.task.TaskRegistry;
import com.neverwinterdp.scribengin.dataflow.task.DataflowTaskConfig;

public class DataflowTaskRegistry extends TaskRegistry<DataflowTaskConfig> {
  
  public DataflowTaskRegistry(Registry registry, String path) throws RegistryException {
    init(registry, path, DataflowTaskConfig.class) ;
  }
}
