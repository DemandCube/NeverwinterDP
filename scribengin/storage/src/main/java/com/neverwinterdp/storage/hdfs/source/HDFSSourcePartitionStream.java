package com.neverwinterdp.storage.hdfs.source;

import java.io.IOException;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.ssm.SSMTagDescriptor;
import com.neverwinterdp.ssm.hdfs.HdfsSSM;
import com.neverwinterdp.ssm.hdfs.HdfsSSMReader;
import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.hdfs.HDFSStorageTag;
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
  
  public StorageConfig getStorageConfig() { return storageConfig; }
  
  public PartitionStreamConfig getPartitionStreamConfig() { return partitionConfig ; }
  
  @Override
  public  HDFSSourcePartitionStreamReader getReader(String name) throws RegistryException, IOException {
    HdfsSSMReader hdfsSSMReader = (HdfsSSMReader) partitionStorage.getReader(name);
    return new HDFSSourcePartitionStreamReader(name, hdfsSSMReader, partitionConfig) ;
  }
  
  public  HDFSSourcePartitionStreamReader getReader(String name, HDFSStorageTag tag) throws RegistryException, IOException {
    SSMTagDescriptor pTag = tag.getPartitionTagDescriptors().get(partitionConfig.getPartitionStreamId());
    HdfsSSMReader hdfsSSMReader = 
        (HdfsSSMReader) partitionStorage.getReader(name, pTag.getSegmentId(), pTag.getSegmentRecordPosition());
    return new HDFSSourcePartitionStreamReader(name, hdfsSSMReader, partitionConfig) ;
  }
}
