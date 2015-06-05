package com.neverwinterdp.scribengin.storage.es.sink;

import com.neverwinterdp.es.ESClient;
import com.neverwinterdp.es.ESObjectClient;
import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.es.ESStorage;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;
import com.neverwinterdp.util.log.Log4jRecord;

public class ESStreamRawWriter implements SinkStreamWriter {
  ESStorage        storage ;
  StreamDescriptor descriptor;
  ESObjectClient<Record> esRecordClient;
  
  public ESStreamRawWriter(StreamDescriptor descriptor) {
    this.storage = new ESStorage(descriptor);
    this.descriptor = descriptor;
    esRecordClient =
        new ESObjectClient<Record>(new ESClient(storage.getAddress()), storage.getIndexName(), Log4jRecord.class) ;
  }
  
  @Override
  public void append(Record record) throws Exception {
    esRecordClient.put(record, record.getKey());
  }


  @Override
  public void close() throws Exception {
    esRecordClient.close();
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
