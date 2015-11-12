
package com.neverwinterdp.scribengin.storage.s3.source;

import com.neverwinterdp.scribengin.storage.PartitionStreamConfig;
import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.s3.S3Client;
import com.neverwinterdp.scribengin.storage.source.SourcePartitionStream;

public class S3SourcePartitionStream implements SourcePartitionStream {
  private S3Client              s3Client ;
  private StorageConfig         storageConfig;
  private String                partitionName;
  private PartitionStreamConfig partitionStreamConfig ;
  
  public S3SourcePartitionStream(S3Client s3Client, StorageConfig sConfig, 
                                 String partitionName, PartitionStreamConfig pConfig) {
    this.s3Client              = s3Client;
    this.storageConfig         = sConfig;
    this.partitionName         = partitionName;
    this.partitionStreamConfig = pConfig;
  }

  public PartitionStreamConfig getPartitionStreamConfig() { return partitionStreamConfig ; }
  
  @Override
  public S3SourcePartitionStreamReader getReader(String name) throws Exception {
    return new S3SourcePartitionStreamReader(name, s3Client, storageConfig, partitionName, partitionStreamConfig) ;
  }
}
