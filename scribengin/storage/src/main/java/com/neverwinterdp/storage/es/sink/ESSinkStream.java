package com.neverwinterdp.storage.es.sink;

import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.es.ESStorage;
import com.neverwinterdp.storage.sink.SinkPartitionStream;
import com.neverwinterdp.storage.sink.SinkPartitionStreamWriter;

public class ESSinkStream implements SinkPartitionStream {
  private ESStorage             esStorage;
  private PartitionStreamConfig partitionConfig;

  public ESSinkStream(ESStorage esStorage, PartitionStreamConfig pConfig) {
    this.esStorage       = esStorage;
    this.partitionConfig = pConfig;
  }
  
  public int getPartitionStreamId() { return partitionConfig.getPartitionStreamId(); }
  
  @Override
  public void delete() throws Exception {
  }

  @Override
  public SinkPartitionStreamWriter getWriter() throws Exception { 
    return new ESStreamWriter(esStorage, partitionConfig); 
  }

  @Override
  public void optimize() throws Exception {
  }
}
