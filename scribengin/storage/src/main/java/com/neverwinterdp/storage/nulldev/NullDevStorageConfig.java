package com.neverwinterdp.storage.nulldev;

import com.neverwinterdp.storage.StorageConfig;

public class NullDevStorageConfig extends StorageConfig {
  
  public NullDevStorageConfig() { 
    setType("nulldev");
    attribute("name", "nulldev");
  }
  
  public NullDevStorageConfig(StorageConfig config) { 
    putAll(config);
  }
}
