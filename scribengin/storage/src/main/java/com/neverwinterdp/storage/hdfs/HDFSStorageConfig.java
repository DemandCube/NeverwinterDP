package com.neverwinterdp.storage.hdfs;

import com.neverwinterdp.storage.StorageConfig;

@SuppressWarnings("serial")
public class HDFSStorageConfig extends StorageConfig {
  final static public String NAME          = "name";
  final static public String REGISTRY_PATH = "registry.path" ;
  final static public String LOCATION      = "location" ;
  

  public HDFSStorageConfig() { 
    setType("hdfs");
  }
  
  public HDFSStorageConfig(StorageConfig config) { 
    putAll(config);
  }
  
  public HDFSStorageConfig(String name, String registryPath, String location) { 
    setType("hdfs");
    setName(name);
    setRegistryPath(registryPath);
    setLocation(location);
  }
  
  public String getName() { return attribute(NAME); }
  public void   setName(String name) { attribute(NAME, name); }
 
  public String getRegistryPath() { return attribute(REGISTRY_PATH); }
  public void   setRegistryPath(String path) { attribute(REGISTRY_PATH, path); }
 
  public String getLocation() { return attribute(LOCATION); }
  public void   setLocation(String location) { attribute(LOCATION, location); }
 
}
