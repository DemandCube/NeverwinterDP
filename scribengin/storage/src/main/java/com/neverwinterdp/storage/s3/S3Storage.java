package com.neverwinterdp.storage.s3;

import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.Storage;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.s3.sink.S3Sink;
import com.neverwinterdp.storage.s3.source.S3Source;

public class S3Storage extends Storage {
  final static public String BUCKET_NAME   = "s3.bucket.name";
  final static public String STORAGE_PATH  = "s3.storage.path";
  
  private String bucketName ;
  private String storageFolder ;
  
  private S3Client s3Client ;
  
  public S3Storage(S3Client s3Client, StorageConfig sConfig) {
    super(sConfig);
    this.bucketName    = sConfig.attribute(BUCKET_NAME);
    this.storageFolder = sConfig.attribute(STORAGE_PATH);
    this.s3Client      = s3Client;
  }
  
  public String getBucketName() { return this.bucketName ; }
  
  public String getStorageFolder() { return this.storageFolder ; }
  
  public PartitionStreamConfig createPartitionConfig(String streamKey) {
    int partitionId = Integer.parseInt(streamKey.substring(streamKey.lastIndexOf('-') + 1)) ;
    PartitionStreamConfig descriptor = new PartitionStreamConfig(partitionId, bucketName) ;
    descriptor.attribute(BUCKET_NAME,  bucketName);
    descriptor.attribute(STORAGE_PATH, storageFolder);
    return descriptor;
  }
  
  public String getPartitionKey(PartitionStreamConfig pConfig) {
    return storageFolder + "/stream-" + pConfig.getPartitionStreamId();
  }
  
  public PartitionStreamConfig createPartitionConfig(int partitionId) {
    PartitionStreamConfig streamConfig = new PartitionStreamConfig(partitionId, bucketName) ;
    streamConfig.attribute("s3.bucket.name", bucketName);
    streamConfig.attribute("s3.storage.path", storageFolder);
    
    streamConfig.attribute("s3.storage.stream", "stream-" + partitionId);
    return streamConfig;
  }
  
  public S3Client getS3Client() {
    S3Client s3Client = new S3Client();
    return s3Client;
  }
  
  public S3Sink getSink() { 
    return new S3Sink(s3Client, getStorageConfig()); 
  }
  
  public S3Sink getSink(S3Client s3Client) { 
    return new S3Sink(s3Client, getStorageConfig()); 
  }
  
  public S3Source getSource() throws Exception {
    StorageConfig storageConfig = getStorageConfig();
    return new S3Source(s3Client, storageConfig);
  }
  
  @Override
  public void refresh() throws Exception {
  }

  @Override
  public boolean exists() throws Exception {
    return s3Client.hasKey(bucketName, storageFolder);
  }

  @Override
  public void drop() throws Exception {
    s3Client.deleteKeyWithPrefix(bucketName, storageFolder);
  }

  @Override
  public void create() throws Exception {
    if(!s3Client.hasBucket(bucketName)) {
      s3Client.createBucket(bucketName);
    }
    s3Client.createS3Folder(bucketName, storageFolder);
  }
}