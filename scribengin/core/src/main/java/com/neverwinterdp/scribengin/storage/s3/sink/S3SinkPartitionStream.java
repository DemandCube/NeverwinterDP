package com.neverwinterdp.scribengin.storage.s3.sink;

import com.neverwinterdp.scribengin.storage.PartitionStreamConfig;
import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.s3.S3Client;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStream;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStreamWriter;

public class S3SinkPartitionStream implements SinkPartitionStream {
  private S3Client        s3Client;
  private StorageConfig   storageConfig;
  private PartitionStreamConfig partitionStreamConfig;

  public S3SinkPartitionStream(S3Client s3Client, StorageConfig sConfig, PartitionStreamConfig pConfig) {
    this.s3Client              = s3Client;
    this.storageConfig         = sConfig;
    this.partitionStreamConfig = pConfig;
  }
  
  @Override
  public int getPartitionStreamId() { return partitionStreamConfig.getPartitionStreamId(); }
  
  @Override
  public void delete() throws Exception {
  }

  @Override
  public SinkPartitionStreamWriter getWriter() throws Exception {
    return new S3SinkPartitionStreamWriter(s3Client, storageConfig, partitionStreamConfig);
  }

  public void optimize() throws Exception {
  }
}