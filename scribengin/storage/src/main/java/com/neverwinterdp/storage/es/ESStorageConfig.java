package com.neverwinterdp.storage.es;

import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.util.text.StringUtil;

@SuppressWarnings("serial")
public class ESStorageConfig extends StorageConfig {
  final static public String NAME         = "name";
  final static public String INDEX        = "indexName";
  final static public String MAPPING_TYPE = "mappingType";
  final static public String ADDRESSES    = "addresses";
  
  public ESStorageConfig() { 
    setType("es");
  }
  
  public ESStorageConfig(StorageConfig config) { 
    putAll(config);
  }
  
  public ESStorageConfig(String name, String index, String addresses, Class<?> mappingType) { 
    this(name, index, addresses, mappingType.getName());
  }

  public ESStorageConfig(String name, String index, String[] address, Class<?> mappingType) { 
    this(name, index, StringUtil.joinStringArray(address), mappingType.getName());
  }
  
  public ESStorageConfig(String name, String index, String addresses, String mappingType) { 
    setType("es");
    setName(name);
    setIndex(index);
    setAddresses(addresses);
    setMappingType(mappingType);
  }
  
  public String getName() { return attribute(NAME); }
  public void   setName(String name) { attribute(NAME, name); }
 
  public String getIndex() { return attribute(INDEX); }
  public void   setIndex(String index) { attribute(INDEX, index); }
  
  public String getAddresses() { return attribute(ADDRESSES); }
  public void   setAddresses(String host) { attribute(ADDRESSES, host); }
  
  public String[] stringArrayAddress() { return this.stringArrayAttribute(ADDRESSES, null); }
  
  public String getMappingType() { return attribute(MAPPING_TYPE); }
  public void   setMappingType(String mappingType) { attribute(MAPPING_TYPE, mappingType); }
  
}
