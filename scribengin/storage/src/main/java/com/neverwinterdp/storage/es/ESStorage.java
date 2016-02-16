package com.neverwinterdp.storage.es;

import com.neverwinterdp.es.ESClient;
import com.neverwinterdp.es.ESObjectClient;
import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.Storage;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.es.sink.ESSink;
import com.neverwinterdp.storage.source.Source;

public class ESStorage extends Storage {
  private Class<?> mappingTypeClass;
  
  public ESStorage(String name, String[] address, String indexName, String mapping) throws Exception {
    this(new ESStorageConfig());
  }
  
  public ESStorage(String name, String[] address, String indexName, String mapping, StorageConfig storageConfig) throws Exception {
    this(new ESStorageConfig());
  }
  
  public ESStorage(ESStorageConfig esStorageConfig) throws Exception {
    super(esStorageConfig);
    mappingTypeClass = Class.forName(esStorageConfig.getMappingType());
  }
  
  public ESStorage(StorageConfig config) {
    super(config);
  }
  
  public Class<?> getMappingClass() { return mappingTypeClass; }
  
  public ESStorageConfig getESStorageConfig() { return new ESStorageConfig(getStorageConfig());  }
  
  public PartitionStreamConfig newStreamDescriptor() {
    PartitionStreamConfig descriptor = new PartitionStreamConfig(getStorageConfig()) ;
    return descriptor;
  }
  
  public ESObjectClient<Object> getESObjectClient() throws Exception {
    ESStorageConfig esStorageConfig = getESStorageConfig();
    ESClient esClient = new ESClient(esStorageConfig.stringArrayAddress());
    return new ESObjectClient<Object>(esClient, esStorageConfig.getIndex(), mappingTypeClass);
  }

  @Override
  public void refresh() throws Exception {
  }

  @Override
  public boolean exists() throws Exception {
    ESStorageConfig esStorageConfig = getESStorageConfig();
    ESClient esClient = new ESClient(esStorageConfig.stringArrayAddress());
    return esClient.hasIndex(esStorageConfig.getIndex());
  }

  @Override
  public void drop() throws Exception {
  }

  @Override
  public void create() throws Exception {
  }

  @Override
  public ESSink getSink() { return new ESSink(this); }

  @Override
  public Source getSource() { 
    throw new RuntimeException("Not Supported!!!"); 
  }
}