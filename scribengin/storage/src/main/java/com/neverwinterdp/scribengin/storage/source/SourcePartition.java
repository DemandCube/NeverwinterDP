package com.neverwinterdp.scribengin.storage.source;

import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.PartitionStreamConfig;

/**
 * @author Tuan Nguyen
 */
public interface SourcePartition {
  public StorageConfig getStorageConfig() ;
  public SourcePartitionStream   getPartitionStream(int id) throws Exception ;
  public SourcePartitionStream   getPartitionStream(PartitionStreamConfig descriptor) throws Exception ;
  
  public SourcePartitionStream[] getPartitionStreams() throws Exception ;
  
  public void close() throws Exception ;
}