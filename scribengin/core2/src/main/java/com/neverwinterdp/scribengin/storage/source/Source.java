package com.neverwinterdp.scribengin.storage.source;

import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.PartitionConfig;

/**
 * @author Tuan Nguyen
 */
public interface Source {
  public StorageConfig getDescriptor() ;
  public SourcePartitionStream   getStream(int id) ;
  public SourcePartitionStream   getStream(PartitionConfig descriptor) ;
  
  public SourcePartitionStream[] getStreams() ;
  
  public void close() throws Exception ;
}