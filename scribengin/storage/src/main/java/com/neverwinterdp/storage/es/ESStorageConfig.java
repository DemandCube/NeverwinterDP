package com.neverwinterdp.storage.es;

import com.neverwinterdp.storage.StorageConfig;

@SuppressWarnings("serial")
public class ESStorageConfig extends StorageConfig {
  final static public String NAME        = "name";
  final static public String INDEX       = "indexName";
  final static public String MAPPING     = "mappingType";
  final static public String HOST        = "host";
  
  
  public ESStorageConfig() { 
    setType("es");
  }
  
  public ESStorageConfig(StorageConfig config) { 
    putAll(config);
  }
  
  public ESStorageConfig(String name, String index, String host, String mapping) { 
    setType("es");
    setName(name);
    setIndex(index);
    setHost(host);
    setMapping(mapping);
  }
  
  public String getName() { return attribute(NAME); }
  public void   setName(String name) { attribute(NAME, name); }
 
  public String getIndex() { return attribute(INDEX); }
  public void   setIndex(String index) { attribute(INDEX, index); }
  
  public String getHost() { return attribute(HOST); }
  public void   setHost(String host) { attribute(HOST, host); }
  
  public String getMapping() { return attribute(MAPPING); }
  public void   setMapping(String mapping) { attribute(MAPPING, mapping); }
  
}
