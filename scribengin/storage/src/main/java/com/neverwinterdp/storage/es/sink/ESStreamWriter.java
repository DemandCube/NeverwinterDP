package com.neverwinterdp.storage.es.sink;

import com.neverwinterdp.es.ESObjectClient;
import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.Record;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.es.ESStorage;
import com.neverwinterdp.storage.sink.SinkPartitionStreamWriter;
import com.neverwinterdp.util.JSONSerializer;

public class ESStreamWriter implements SinkPartitionStreamWriter {
  ESStorage        storage ;
  PartitionStreamConfig  partitionConfig;
  ESObjectClient<Object> esObjClient;
  
  public ESStreamWriter(StorageConfig sConfig, PartitionStreamConfig pConfig) throws Exception {
    this.storage = new ESStorage(sConfig);
    this.partitionConfig = pConfig;
    esObjClient = storage.getESObjectClient();
  }
  
  @Override
  public void append(Record dataflowMessage) throws Exception {
    Object obj = JSONSerializer.INSTANCE.fromBytes(dataflowMessage.getData(), storage.getMappingType());
    esObjClient.put(obj, dataflowMessage.getKey());
  }


  @Override
  public void close() throws Exception {
    esObjClient.close();
  }

  @Override
  public void rollback() throws Exception {
  }

  @Override
  public void commit() throws Exception {
  }

  @Override
  public void prepareCommit() {
  }

  @Override
  public void completeCommit() {
  }
}
