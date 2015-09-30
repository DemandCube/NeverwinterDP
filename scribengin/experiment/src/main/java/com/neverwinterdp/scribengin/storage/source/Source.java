package com.neverwinterdp.scribengin.storage.source;

import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.PartitionDescriptor;

/**
 * @author Tuan Nguyen
 */
public interface Source {
  public StorageDescriptor getDescriptor() ;
  public SourcePartitionStream   getStream(int id) ;
  public SourcePartitionStream   getStream(PartitionDescriptor descriptor) ;
  
  public SourcePartitionStream[] getStreams() ;
  
  public void close() throws Exception ;
}