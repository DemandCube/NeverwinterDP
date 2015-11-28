package com.neverwinterdp.nstoragebak;

import java.util.List;

import com.neverwinterdp.registry.RegistryException;

public class NStoragePartitionReader<T> {
  private NStorageContext<T>                context;
  private NStoragePartition                 partition;
  private NStoragePartitionSegmentReader<T> currentSegmentReader;
  
  public NStoragePartitionReader(NStorageContext<T> context, NStoragePartition  partition) throws RegistryException {
    this.context   = context;
    this.partition = partition ;
    
    NStorageRegistry<T> hqueueRegistry = context.getNStorageRegistry();
    List<NStoragePartitionSegment> segments = hqueueRegistry.getNStoragePartitionSegments(partition);
  }
  
  public T next(long maxWaitTime) throws Exception {
    return null ;
  }
  
  public void close() {
    
  }
}