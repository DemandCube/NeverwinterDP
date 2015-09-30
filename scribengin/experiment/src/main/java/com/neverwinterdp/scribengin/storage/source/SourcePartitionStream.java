package com.neverwinterdp.scribengin.storage.source;

import com.neverwinterdp.scribengin.storage.PartitionDescriptor;

public interface SourcePartitionStream {
  public PartitionDescriptor getDescriptor() ;
  public SourcePartitionStreamReader  getReader(String name) throws Exception ;
}
