package com.neverwinterdp.scribengin.storage.s3.sink;

import com.neverwinterdp.scribengin.storage.PartitionStreamConfig;
import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.s3.S3Folder;
import com.neverwinterdp.scribengin.storage.s3.S3Storage;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStream;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStreamWriter;

public class S3SinkPartitionStream implements SinkPartitionStream {
  private S3Folder        s3StreamFolder;
  private StorageConfig   storageConfig;
  private PartitionStreamConfig partitionStreamConfig;

  public S3SinkPartitionStream(S3Folder s3SinkFolder, StorageConfig sConfig, PartitionStreamConfig pConfig) {
    this.storageConfig = sConfig;
    this.partitionStreamConfig = pConfig;
    S3Storage storage = new S3Storage(storageConfig);
    String streamKey = storage.getPartitionKey(pConfig);
    if(s3SinkFolder.hasChild(streamKey)) {
      s3StreamFolder = s3SinkFolder.getS3Folder(streamKey); 
    } else {
      s3StreamFolder = s3SinkFolder.createFolder(streamKey) ;
    }
  }
  
  @Override
  public int getPartitionStreamId() { return partitionStreamConfig.getPartitionStreamId(); }
  
  @Override
  public void delete() throws Exception {
  }

  @Override
  public SinkPartitionStreamWriter getWriter() throws Exception {
    return new S3SinkPartitionStreamWriter(s3StreamFolder);
  }

  public void optimize() throws Exception {
  }
  
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("S3SinkStream [streamS3Folder=");
    builder.append(s3StreamFolder);
    builder.append(", descriptor=");
    builder.append(partitionStreamConfig);
    builder.append("]");
    return builder.toString();
  }
}