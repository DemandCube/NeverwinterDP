package com.neverwinterdp.scribengin.storage.es;

import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
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
  
  public ESStorage(StorageDescriptor descriptor) {
    fromStorageDescriptor(descriptor);
  }
  
  public ESStorage(StreamDescriptor descriptor) {
    fromStorageDescriptor(descriptor);
  }
  
  public String[] getAddress() { return address ; }
  
  public String getIndexName() { return indexName; }
  
  public Class<?> getMappingType() { return mappingType; }
  
  public StorageDescriptor getStorageDescriptor() { return toStorageDesciptor();  }
  
  public StreamDescriptor newStreamDescriptor() {
    StreamDescriptor descriptor = new StreamDescriptor(getStorageDescriptor()) ;
    return descriptor;
  }
  
  StorageDescriptor toStorageDesciptor() {
    StorageDescriptor descriptor = new StorageDescriptor("elasticsearch") ;
    descriptor.attribute("address", StringUtil.joinStringArray(address)) ;
    descriptor.attribute("indexName", indexName) ;
    descriptor.attribute("mappingType", mappingType.getName()) ;
    return descriptor ;
 }
  
  void fromStorageDescriptor(StorageDescriptor descriptor) {
    try {
      address = StringUtil.toStringArray(descriptor.attribute("address"));
      indexName = descriptor.attribute("indexName");
      mappingType = Class.forName(descriptor.attribute("mappingType"));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}