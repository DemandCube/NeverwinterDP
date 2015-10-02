package com.neverwinterdp.hqueue;

import java.util.List;

import com.neverwinterdp.registry.RegistryException;

public class HQueuePartitionWriter<T> {
  private HQueueContext<T>                context;
  private HQueuePartition                 partition;
  private HQueuePartitionSegmentWriter<T> currentSegmentWriter;

  public HQueuePartitionWriter(HQueueContext<T> context, HQueuePartition  partition) throws RegistryException {
    this.context   = context;
    this.partition = partition ;
    
    HQueueRegistry<T> hqueueRegistry = context.getHQueueRegistry();
    List<HQueuePartitionSegment> segments = hqueueRegistry.getHQueuePartitionSegments(partition);
    HQueuePartitionSegment currentSegment = null;
    if(segments.size() > 0) {
      currentSegment = segments.get(segments.size() - 1);
    }
    if(currentSegment == null) {
      currentSegment = hqueueRegistry.newHQueuePartitionSegment(partition);
    }
    currentSegmentWriter = new HQueuePartitionSegmentWriter<T>(context, partition, currentSegment);
  }
  
  public void write(T obj) throws RegistryException, HQueueException {
    currentSegmentWriter.append(obj);
    if(currentSegmentWriter.isFull()) {
      currentSegmentWriter.commit();
      currentSegmentWriter.close();
      HQueueRegistry<T> hqueueRegistry = context.getHQueueRegistry();
      HQueuePartitionSegment currentSegment = hqueueRegistry.newHQueuePartitionSegment(partition);
      currentSegmentWriter = new HQueuePartitionSegmentWriter<T>(context, partition, currentSegment);
    }
  }
  
  public void commit() throws RegistryException, HQueueException {
    currentSegmentWriter.commit();
  }
  
  public void rollback() throws RegistryException, HQueueException {
    currentSegmentWriter.rollback();
  }
  
  public void close() throws RegistryException, HQueueException {
    currentSegmentWriter.close();
  }
}