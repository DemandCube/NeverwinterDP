package com.neverwinterdp.storage.es.sink;

import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.sink.SinkPartitionStream;
import com.neverwinterdp.storage.sink.SinkPartitionStreamWriter;

public class ESSinkStream implements SinkPartitionStream {
  private StorageConfig         storageConfig;
  private PartitionStreamConfig partitionConfig;

  public ESSinkStream(StorageConfig   sConfig, PartitionStreamConfig pConfig) {
    this.storageConfig = sConfig;
    this.partitionConfig = pConfig;
  }
  
  public int getPartitionStreamId() { return partitionConfig.getPartitionStreamId(); }
  
  @Override
  public void delete() throws Exception {
  }

  @Override
  public SinkPartitionStreamWriter getWriter() throws Exception { 
    return new ESStreamWriter(storageConfig, partitionConfig); 
  }

  @Override
  public void optimize() throws Exception {
  }
}
