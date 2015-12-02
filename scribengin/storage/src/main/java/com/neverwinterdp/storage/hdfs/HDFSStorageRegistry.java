package com.neverwinterdp.storage.hdfs;

import com.neverwinterdp.registry.Registry;

public class HDFSStorageRegistry {
  private Registry registry ;
  private String   registryPath ;
  
  public HDFSStorageRegistry(Registry registry, String registryPath) {
    this.registry = registry;
    this.registryPath = registryPath;
  }
  
  public Registry getRegistry() { return registry ; }
  
  public String getRegistryPath() { return registryPath ; }
}
