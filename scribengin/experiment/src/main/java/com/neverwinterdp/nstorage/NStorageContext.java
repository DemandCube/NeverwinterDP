package com.neverwinterdp.nstorage;

import java.util.List;

import org.apache.hadoop.fs.FileSystem;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;

public class NStorageContext<T> {
  private NStorageConfig<T>   nstorage;
  private NStorageRegistry<T> nstorageRegistry;
  private FileSystem          fs;

  public NStorageContext(Registry registry, String path) throws RegistryException {
    nstorageRegistry = new NStorageRegistry<T>(registry, path);
    nstorage = nstorageRegistry.getNStorageConfig();
  }
  
  public NStorageContext(Registry registry, NStorageConfig<T> hqueue) throws RegistryException {
    nstorageRegistry = new NStorageRegistry<T>(registry, hqueue);
    this.nstorage = hqueue;
  }
  
  public NStorageConfig<T> getHQueue() { return nstorage ; }
  
  public NStorageRegistry<T> getNStorageRegistry() { return nstorageRegistry; }

  public NStoragePartition getPartition(int partitionId) throws RegistryException { 
    return nstorageRegistry.getPartition(partitionId); 
  }
  
  public List<NStoragePartition> getPartitions() throws RegistryException { 
    return nstorageRegistry.getPartitions(); 
  }
  
  public NStoragePartitionReader<T> getPartitionReader(NStoragePartition partition) throws RegistryException, NStorageException {
    NStoragePartitionReader<T> reader = new NStoragePartitionReader<T>(this, partition);
    return reader ;
  }
  
  public NStoragePartitionWriter<T> getPartitionWriter(NStoragePartition partition) throws RegistryException, NStorageException {
    NStoragePartitionWriter<T> writer = new NStoragePartitionWriter<T>(this, partition);
    return writer ;
  }
}
