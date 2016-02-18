package com.neverwinterdp.storage.es.sink;

import com.neverwinterdp.es.ESObjectClient;
import com.neverwinterdp.message.Message;
import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.es.ESStorage;
import com.neverwinterdp.storage.sink.SinkPartitionStreamWriter;
import com.neverwinterdp.util.JSONSerializer;

public class ESStreamWriter implements SinkPartitionStreamWriter {
  private ESStorage              storage;
  private PartitionStreamConfig  partitionConfig;
  private ESObjectClient<Object> esObjClient;
  private Class<?>               mappingTypeClass ;
  
  public ESStreamWriter(ESStorage esStorage, PartitionStreamConfig pConfig) throws Exception {
    this.storage = esStorage;
    this.partitionConfig = pConfig;
    mappingTypeClass = esStorage.getMappingTypeClass();
    esObjClient = storage.getESObjectClient();
  }
  
  @Override
  public void append(Message message) throws Exception {
    Object obj = JSONSerializer.INSTANCE.fromBytes(message.getData(), mappingTypeClass);
    esObjClient.put(obj, message.getKey());
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
