package com.neverwinterdp.scribengin.storage.es.sink;

import com.neverwinterdp.es.ESObjectClient;
import com.neverwinterdp.scribengin.storage.Record;
import com.neverwinterdp.scribengin.storage.PartitionDescriptor;
import com.neverwinterdp.scribengin.storage.es.ESStorage;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStreamWriter;
import com.neverwinterdp.util.JSONSerializer;

public class ESStreamWriter implements SinkPartitionStreamWriter {
  ESStorage        storage ;
  PartitionDescriptor descriptor;
  ESObjectClient<Object> esObjClient;
  
  public ESStreamWriter(PartitionDescriptor descriptor) throws Exception {
    this.storage = new ESStorage(descriptor);
    this.descriptor = descriptor;
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
