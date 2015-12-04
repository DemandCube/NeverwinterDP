package com.neverwinterdp.storage.hdfs.source;

import java.io.IOException;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.ssm.hdfs.HdfsSSM;
import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.source.SourcePartitionStream;

public class HDFSSourcePartitionStream implements SourcePartitionStream {
  private HdfsSSM                partitionStorage;
  private StorageConfig          storageConfig;
  private PartitionStreamConfig  partitionConfig;
  
  public HDFSSourcePartitionStream(HdfsSSM pStorage, StorageConfig sConfig, PartitionStreamConfig pConfig) {
    this.partitionStorage  = pStorage;
    this.storageConfig     = sConfig;
    this.partitionConfig   = pConfig;
  }
  
  public PartitionStreamConfig getPartitionStreamConfig() { return partitionConfig ; }
  
  @Override
  public  HDFSSourcePartitionStreamReader getReader(String name) throws RegistryException, IOException {
    return new HDFSSourcePartitionStreamReader(name, partitionStorage, storageConfig, partitionConfig) ;
  }

  public void deleteReadDataByActiveReader() throws RegistryException, IOException {
    partitionStorage.cleanReadSegmentByActiveReader();
  }
}
