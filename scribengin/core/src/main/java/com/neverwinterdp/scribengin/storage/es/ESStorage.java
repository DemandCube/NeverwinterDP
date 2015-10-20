package com.neverwinterdp.scribengin.storage.es;

import com.neverwinterdp.es.ESClient;
import com.neverwinterdp.es.ESObjectClient;
import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.PartitionConfig;
import com.neverwinterdp.util.text.StringUtil;

public class ESStorage {
  private String[] address ;
  private String   indexName ;
  private Class<?> mappingType ;
  
  public ESStorage(String[] address, String indexName, Class<?> type) {
    this.address = address ;
    this.indexName = indexName; 
    this.mappingType = type ;
  }
  
  public ESStorage(StorageConfig descriptor) {
    fromStorageDescriptor(descriptor);
  }
  
  public String[] getAddress() { return address ; }
  
  public String getIndexName() { return indexName; }
  
  public Class<?> getMappingType() { return mappingType; }
  
  public StorageConfig getStorageConfig() { return toStorageConfig();  }
  
  public PartitionConfig newStreamDescriptor() {
    PartitionConfig descriptor = new PartitionConfig(getStorageConfig()) ;
    return descriptor;
  }
  
  public ESObjectClient<Object> getESObjectClient() {
    ESObjectClient<Object> esObjClient = new ESObjectClient<Object>(new ESClient(address), indexName, mappingType) ;
    return esObjClient;
  }

  StorageConfig toStorageConfig() {
    StorageConfig descriptor = new StorageConfig("elasticsearch") ;
    descriptor.attribute("address", StringUtil.joinStringArray(address)) ;
    descriptor.attribute("indexName", indexName) ;
    descriptor.attribute("mappingType", mappingType.getName()) ;
    return descriptor ;
 }
  
  void fromStorageDescriptor(StorageConfig descriptor) {
    try {
      address = StringUtil.toStringArray(descriptor.attribute("address"));
      indexName = descriptor.attribute("indexName");
      mappingType = Class.forName(descriptor.attribute("mappingType"));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}