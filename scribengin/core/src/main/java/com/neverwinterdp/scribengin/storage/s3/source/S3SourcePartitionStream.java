
package com.neverwinterdp.scribengin.storage.s3.source;

import com.neverwinterdp.scribengin.storage.PartitionStreamConfig;
import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.s3.S3Client;
import com.neverwinterdp.scribengin.storage.source.SourcePartitionStream;
import com.neverwinterdp.scribengin.storage.source.SourcePartitionStreamReader;

public class S3SourcePartitionStream implements SourcePartitionStream {
  private S3Client s3Client ;
  private StorageConfig   storageConfig;
  private PartitionStreamConfig partitionConfig ;
  
  public S3SourcePartitionStream(S3Client s3Client, StorageConfig sConfig, PartitionStreamConfig pConfig) {
    this.s3Client= s3Client;
    this.partitionConfig = pConfig;
    this.storageConfig = sConfig;
  }

  public PartitionStreamConfig getDescriptor() { return partitionConfig ; }
  
  @Override
  public SourcePartitionStreamReader getReader(String name) throws Exception {
    return new S3SourcePartitionStreamReader(name, s3Client, storageConfig, partitionConfig) ;
  }
}
