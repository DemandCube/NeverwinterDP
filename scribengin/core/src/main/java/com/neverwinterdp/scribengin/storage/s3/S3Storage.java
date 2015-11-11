package com.neverwinterdp.scribengin.storage.s3;

import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.PartitionStreamConfig;
import com.neverwinterdp.scribengin.storage.s3.sink.S3Sink;
import com.neverwinterdp.scribengin.storage.s3.source.S3SourcePartition;

public class S3Storage {
  private String bucketName ;
  private String storageFolder ;
  
  public S3Storage(String bucketName, String storageFolder) {
    this.bucketName = bucketName;
    this.storageFolder = storageFolder;
  }
  
  public S3Storage(StorageConfig descriptor) {
    fromStorageDescriptor(descriptor);
  }
  
  public String getBucketName() { return this.bucketName ; }
  
  public String getStorageFolder() { return this.storageFolder ; }
  
  public StorageConfig getStorageDescriptor() { return toStorageDesciptor();  }
  
  public PartitionStreamConfig createPartitionConfig(String streamKey) {
    int id = Integer.parseInt(streamKey.substring(streamKey.lastIndexOf('-') + 1)) ;
    PartitionStreamConfig descriptor = new PartitionStreamConfig(id, bucketName) ;
    descriptor.attribute("s3.bucket.name", bucketName);
    descriptor.attribute("s3.storage.path", storageFolder);
    return descriptor;
  }
  
  public String getPartitionKey(PartitionStreamConfig pConfig) {
    return this.storageFolder + "/stream-" + pConfig.getPartitionStreamId();
  }
  
  public PartitionStreamConfig createPartitionConfig(int id) {
    PartitionStreamConfig descriptor = new PartitionStreamConfig(id, bucketName) ;
    descriptor.attribute("s3.bucket.name", bucketName);
    descriptor.attribute("s3.storage.path", storageFolder);
    descriptor.attribute("s3.storage.stream", "stream-" + id);
    return descriptor;
  }
  
  public S3Client getS3Client() {
    S3Client s3Client = new S3Client();
    return s3Client;
  }
  
  public S3Sink getSink() { 
    return new S3Sink(getStorageDescriptor()); 
  }
  
  public S3Sink getSink(S3Client s3Client) { 
    return new S3Sink(s3Client, getStorageDescriptor()); 
  }
  
  public S3SourcePartition getSource() throws Exception { 
    return new S3SourcePartition(getS3Client(), toStorageDesciptor()); 
  }
  
  public S3SourcePartition getSource(S3Client s3Client) throws Exception { 
    return new S3SourcePartition(getS3Client(), toStorageDesciptor()); 
  }
  
  StorageConfig toStorageDesciptor() {
    StorageConfig descriptor = new StorageConfig("S3") ;
    descriptor.attribute("s3.bucket.name", bucketName);
    descriptor.attribute("s3.storage.path", storageFolder);
    return descriptor ;
 }
  
  void fromStorageDescriptor(StorageConfig descriptor) {
    bucketName = descriptor.attribute("s3.bucket.name");
    storageFolder = descriptor.attribute("s3.storage.path");
  }
}