package com.neverwinterdp.storage.source;

import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.StorageConfig;

/**
 * @author Tuan Nguyen
 */
public interface SourcePartition {
  public StorageConfig getStorageConfig() ;
  public SourcePartitionStream getPartitionStream(int id) throws Exception ;
  public SourcePartitionStream getPartitionStream(PartitionStreamConfig descriptor) throws Exception ;
  
  public SourcePartitionStream[] getPartitionStreams() throws Exception ;
  
  public void close() throws Exception ;
}