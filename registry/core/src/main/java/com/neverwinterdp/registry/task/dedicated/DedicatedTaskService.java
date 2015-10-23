package com.neverwinterdp.registry.task.dedicated;

import javax.annotation.PreDestroy;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;

public class DedicatedTaskService<T> {
  private DedicatedTaskRegistry<T> taskRegistry ;

  public DedicatedTaskService() { }
  
  public DedicatedTaskService(DedicatedTaskRegistry<T> taskRegistry) throws RegistryException {
    init(taskRegistry) ;
  }
  
  public DedicatedTaskService(Registry registry, String path, Class<T> taskDescriptorType) throws RegistryException {
    init(registry, path, taskDescriptorType) ;
  }
  
  protected void init(Registry registry, String path, Class<T> taskDescriptorType) throws RegistryException {
    init(new DedicatedTaskRegistry<T>(registry, path, taskDescriptorType));
  }
  
  protected void init(DedicatedTaskRegistry<T> taskReg) throws RegistryException {
    taskRegistry = taskReg;
    taskReg.initRegistry();
  }
  
  @PreDestroy
  public void onDestroy() {
  } 
  
  public DedicatedTaskRegistry<T> getTaskRegistry() { return this.taskRegistry; }
}
