package com.neverwinterdp.scribengin.storage.source;

import com.neverwinterdp.scribengin.storage.PartitionStreamConfig;

public interface SourcePartitionStream {
  public PartitionStreamConfig getDescriptor() ;
  public SourcePartitionStreamReader  getReader(String name) throws Exception ;
}
