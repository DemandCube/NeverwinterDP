
package com.neverwinterdp.scribengin.storage.s3.source;

import com.neverwinterdp.scribengin.storage.PartitionConfig;
import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.s3.S3Client;
import com.neverwinterdp.scribengin.storage.source.SourcePartitionStream;
import com.neverwinterdp.scribengin.storage.source.SourcePartitionStreamReader;

public class S3SourcePartitionStream implements SourcePartitionStream {
  private S3Client s3Client ;
  private StorageConfig   storageConfig;
  private PartitionConfig partitionConfig ;
  
  public S3SourcePartitionStream(S3Client s3Client, StorageConfig sConfig, PartitionConfig pConfig) {
    this.s3Client= s3Client;
    this.partitionConfig = pConfig;
    this.storageConfig = sConfig;
  }

  public PartitionConfig getDescriptor() { return partitionConfig ; }
  
  @Override
  public SourcePartitionStreamReader getReader(String name) throws Exception {
    return new S3SourcePartitionStreamReader(name, s3Client, storageConfig, partitionConfig) ;
  }
}
