package com.neverwinterdp.nstorage;

import java.util.List;

import com.neverwinterdp.registry.RegistryException;

public class NStoragePartitionWriter<T> {
  private NStorageContext<T>                context;
  private NStoragePartition                 partition;
  private NStoragePartitionSegmentWriter<T> currentSegmentWriter;

  public NStoragePartitionWriter(NStorageContext<T> context, NStoragePartition  partition) throws RegistryException {
    this.context   = context;
    this.partition = partition ;
    
    NStorageRegistry<T> hqueueRegistry = context.getNStorageRegistry();
    List<NStoragePartitionSegment> segments = hqueueRegistry.getNStoragePartitionSegments(partition);
    NStoragePartitionSegment currentSegment = null;
    if(segments.size() > 0) {
      currentSegment = segments.get(segments.size() - 1);
    }
    if(currentSegment == null) {
      currentSegment = hqueueRegistry.newNStoragePartitionSegment(partition);
    }
    currentSegmentWriter = new NStoragePartitionSegmentWriter<T>(context, partition, currentSegment);
  }
  
  public void write(T obj) throws RegistryException, NStorageException {
    currentSegmentWriter.append(obj);
    if(currentSegmentWriter.isFull()) {
      currentSegmentWriter.commit();
      currentSegmentWriter.close();
      NStorageRegistry<T> hqueueRegistry = context.getNStorageRegistry();
      NStoragePartitionSegment currentSegment = hqueueRegistry.newNStoragePartitionSegment(partition);
      currentSegmentWriter = new NStoragePartitionSegmentWriter<T>(context, partition, currentSegment);
    }
  }
  
  public void commit() throws RegistryException, NStorageException {
    currentSegmentWriter.commit();
  }
  
  public void rollback() throws RegistryException, NStorageException {
    currentSegmentWriter.rollback();
  }
  
  public void close() throws RegistryException, NStorageException {
    currentSegmentWriter.close();
  }
}