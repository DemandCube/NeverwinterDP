package com.neverwinterdp.hqueue;

import java.util.List;

import com.neverwinterdp.registry.RegistryException;

public class HQueuePartitionReader<T> {
  private HQueueContext<T>                context;
  private HQueuePartition                 partition;
  private HQueuePartitionSegmentReader<T> currentSegmentReader;
  
  public HQueuePartitionReader(HQueueContext<T> context, HQueuePartition  partition) throws RegistryException {
    this.context   = context;
    this.partition = partition ;
    
    HQueueRegistry<T> hqueueRegistry = context.getHQueueRegistry();
    List<HQueuePartitionSegment> segments = hqueueRegistry.getHQueuePartitionSegments(partition);
  }
  
  public T next(long maxWaitTime) throws Exception {
    return null ;
  }
  
  public void close() {
    
  }
}