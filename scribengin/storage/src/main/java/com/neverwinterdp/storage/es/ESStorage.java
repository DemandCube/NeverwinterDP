package com.neverwinterdp.storage.es;

import com.neverwinterdp.es.ESClient;
import com.neverwinterdp.es.ESObjectClient;
import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.Storage;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.es.sink.ESSink;
import com.neverwinterdp.storage.sink.Sink;
import com.neverwinterdp.storage.source.Source;
import com.neverwinterdp.util.text.StringUtil;

public class ESStorage extends Storage{
  private StorageConfig sConfig;
  private String name;
  private String[] address ;
  private String   indexName ;
  //private Class<?> mappingType ;
  private String mappingType;

  public ESStorage(String name, String[] address, String indexName, String mapping) {
    this(name, address, indexName, mapping, new StorageConfig());
  }
  
  public ESStorage(String name, String[] address, String indexName, String mapping, StorageConfig storageConfig) {
    super(storageConfig);
    this.sConfig = storageConfig;
    this.name = name;
    this.address = address ;
    this.indexName = indexName; 
    this.mappingType = mapping;
    //try {
    //  this.mappingType = Class.forName(mapping);
    //} catch (ClassNotFoundException e) {
    //  e.printStackTrace();
    //}
  }
  
  public ESStorage(StorageConfig descriptor) {
    super(descriptor);
    fromStorageDescriptor(descriptor);
  }
  
  public String[] getAddress() { return address ; }
  
  public String getIndexName() { return indexName; }
  
  public String getMappingType() { return mappingType; }
  public Class<?> getMappingClass() { 
    try {
      return Class.forName(mappingType);
    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } 
    return null;
  }
  
  public ESStorageConfig getStorageConfig() { return new ESStorageConfig(toStorageConfig());  }
  
  public PartitionStreamConfig newStreamDescriptor() {
    PartitionStreamConfig descriptor = new PartitionStreamConfig(getStorageConfig()) ;
    return descriptor;
  }
  
  public ESObjectClient<Object> getESObjectClient() {
    ESObjectClient<Object> esObjClient = null;
    try {
      esObjClient = new ESObjectClient<Object>(new ESClient(address), indexName, Class.forName(mappingType));
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    return esObjClient;
  }

  StorageConfig toStorageConfig() {
    StorageConfig descriptor = new StorageConfig("elasticsearch") ;
    descriptor.attribute("name", name);
    descriptor.attribute("address", StringUtil.joinStringArray(address)) ;
    descriptor.attribute("indexName", indexName) ;
    descriptor.attribute("mappingType", mappingType) ;
    return descriptor ;
 }
  
  void fromStorageDescriptor(StorageConfig descriptor) {
    address = StringUtil.toStringArray(descriptor.attribute("address"));
    indexName = descriptor.attribute("indexName");
    mappingType = descriptor.attribute("mappingType");
  }

  @Override
  public void refresh() throws Exception {
    // TODO Auto-generated method stub
    System.err.println("REFRESH!!!!");
  }

  @Override
  public boolean exists() throws Exception {
    System.err.println("EXISTS!!!!");
    return false;
  }

  @Override
  public void drop() throws Exception {
    System.err.println("DROP!!!!");
    // TODO Auto-generated method stub
    
  }

  @Override
  public void create() throws Exception {
    System.err.println("CREATE!!!!");
    // TODO Auto-generated method stub
    
  }

  @Override
  public Sink getSink() throws Exception {
    System.err.println("GET SINK!!!!");
    return new ESSink(this.name, this.address, this.indexName, Class.forName(this.mappingType), this.sConfig);
  }

  @Override
  public Source getSource() throws Exception {
    System.err.println("GET SOURCE!!!!");
    // TODO Auto-generated method stub
    return null;
  }
}