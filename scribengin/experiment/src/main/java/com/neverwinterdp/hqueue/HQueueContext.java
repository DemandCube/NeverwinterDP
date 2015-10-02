package com.neverwinterdp.hqueue;

import java.util.List;

import org.apache.hadoop.fs.FileSystem;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;

public class HQueueContext<T> {
  private HQueue<T>         hqueue ;
  private HQueueRegistry<T> hqueueRegistry ;
  private FileSystem        fs;

  public HQueueContext(Registry registry, String path) throws RegistryException {
    hqueueRegistry = new HQueueRegistry<T>(registry, path);
    hqueue = hqueueRegistry.getHQueue();
  }
  
  public HQueueContext(Registry registry, HQueue<T> hqueue) throws RegistryException {
    hqueueRegistry = new HQueueRegistry<T>(registry, hqueue);
    this.hqueue = hqueue;
  }
  
  public HQueue<T> getHQueue() { return this.hqueue ; }
  
  public HQueueRegistry<T> getHQueueRegistry() { return this.hqueueRegistry; }

  public HQueuePartition getHQueuePartition(int partitionId) throws RegistryException { 
    return hqueueRegistry.getPartition(partitionId); 
  }
  
  public List<HQueuePartition> getHQueuePartitions() throws RegistryException { 
    return hqueueRegistry.getPartitions(); 
  }
  
  public HQueuePartitionReader<T> getPartitionReader(HQueuePartition partition) throws RegistryException, HQueueException {
    HQueuePartitionReader<T> reader = new HQueuePartitionReader<T>(this, partition);
    return reader ;
  }
  
  public HQueuePartitionWriter<T> getPartitionWriter(HQueuePartition partition) throws RegistryException, HQueueException {
    HQueuePartitionWriter<T> writer = new HQueuePartitionWriter<T>(this, partition);
    return writer ;
  }
  
  
}
