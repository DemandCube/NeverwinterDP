package com.neverwinterdp.scribengin.storage.source;

import com.neverwinterdp.scribengin.storage.PartitionConfig;

public interface SourcePartitionStream {
  public PartitionConfig getDescriptor() ;
  public SourcePartitionStreamReader  getReader(String name) throws Exception ;
}
