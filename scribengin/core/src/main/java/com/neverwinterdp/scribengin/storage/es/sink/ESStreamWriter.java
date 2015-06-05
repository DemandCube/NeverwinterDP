package com.neverwinterdp.scribengin.storage.es.sink;

import com.neverwinterdp.es.ESClient;
import com.neverwinterdp.es.ESObjectClient;
import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.es.ESStorage;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;
import com.neverwinterdp.util.JSONSerializer;

public class ESStreamWriter implements SinkStreamWriter {
  ESStorage        storage ;
  StreamDescriptor descriptor;
  ESObjectClient<Object> esObjClient;
  
  public ESStreamWriter(StreamDescriptor descriptor) throws Exception {
    this.storage = new ESStorage(descriptor);
    this.descriptor = descriptor;
    esObjClient = storage.getESObjectClient();
  }
  
  @Override
  public void append(Record record) throws Exception {
    Object obj = JSONSerializer.INSTANCE.fromBytes(record.getData(), storage.getMappingType());
    esObjClient.put(obj, record.getKey());
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
