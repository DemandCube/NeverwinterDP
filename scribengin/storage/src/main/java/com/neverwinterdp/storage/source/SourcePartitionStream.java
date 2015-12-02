package com.neverwinterdp.storage.source;

import com.neverwinterdp.storage.PartitionStreamConfig;

public interface SourcePartitionStream {
  public PartitionStreamConfig getPartitionStreamConfig() ;
  public SourcePartitionStreamReader  getReader(String name) throws Exception ;
}
