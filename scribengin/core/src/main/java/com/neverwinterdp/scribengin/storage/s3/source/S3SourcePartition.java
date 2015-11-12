package com.neverwinterdp.scribengin.storage.s3.source;

import com.neverwinterdp.scribengin.storage.PartitionStreamConfig;
import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.s3.S3Client;
import com.neverwinterdp.scribengin.storage.source.SourcePartition;

/**
 * @author Tuan Nguyen
 */
public class S3SourcePartition implements SourcePartition {
  private S3Client      s3Client;
  private StorageConfig storageConfig;
  private String        partitionName;

  public S3SourcePartition(S3Client s3Client, StorageConfig sConfig, String partitionName) throws Exception {
    this.s3Client       = s3Client;
    this.storageConfig  = sConfig;
    this.partitionName  = partitionName;
  }

  @Override
  public StorageConfig getStorageConfig() { return storageConfig; }

  @Override
  public S3SourcePartitionStream getPartitionStream(int partitionStreamId) { 
    PartitionStreamConfig streamConfig = new PartitionStreamConfig(partitionStreamId, null);
    return new S3SourcePartitionStream(s3Client, storageConfig, partitionName, streamConfig);
  }

  @Override
  public S3SourcePartitionStream getPartitionStream(PartitionStreamConfig streamConfig) { 
    return new S3SourcePartitionStream(s3Client, storageConfig, partitionName, streamConfig);
  }

  @Override
  public S3SourcePartitionStream[] getPartitionStreams() {
    int numOfStream  = storageConfig.getPartitionStream() ;
    S3SourcePartitionStream[] stream = new S3SourcePartitionStream[numOfStream];
    for(int i = 0; i < numOfStream; i++) {
      stream[i] = getPartitionStream(i);
    }
    return stream;
  }

  public void close() throws Exception {
  }
}
