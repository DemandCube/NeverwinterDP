package com.neverwinterdp.scribengin.storage.es.sink;

import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.sink.SinkStream;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;

public class ESSinkStream implements SinkStream {
  private StreamDescriptor descriptor;
  
  public ESSinkStream(StreamDescriptor descriptor) {
    this.descriptor = descriptor;
  }
  
  @Override
  public StreamDescriptor getPartitionConfig() { return descriptor; }

  @Override
  public void delete() throws Exception {
  }

  @Override
  public SinkStreamWriter getWriter() throws Exception { 
    return new ESStreamWriter(descriptor); 
  }

  @Override
  public void optimize() throws Exception {
  }
}
