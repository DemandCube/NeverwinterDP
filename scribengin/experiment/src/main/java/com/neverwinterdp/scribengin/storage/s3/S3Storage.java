package com.neverwinterdp.scribengin.storage.s3;

import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.s3.sink.S3Sink;
import com.neverwinterdp.scribengin.storage.s3.source.S3Source;

public class S3Storage {
  private String bucketName ;
  private String storageFolder ;
  
  public S3Storage(String bucketName, String storageFolder) {
    this.bucketName = bucketName;
    this.storageFolder = storageFolder;
  }
  
  public S3Storage(StorageDescriptor descriptor) {
    fromStorageDescriptor(descriptor);
  }
  
  public S3Storage(StreamDescriptor descriptor) {
    fromStorageDescriptor(descriptor);
  }
  
  public String getBucketName() { return this.bucketName ; }
  
  public String getStorageFolder() { return this.storageFolder ; }
  
  public StorageDescriptor getStorageDescriptor() { return toStorageDesciptor();  }
  
  public StreamDescriptor createStreamDescriptor(String streamKey) {
    int id = Integer.parseInt(streamKey.substring(streamKey.lastIndexOf('-') + 1)) ;
    StreamDescriptor descriptor = new StreamDescriptor("S3", id, bucketName) ;
    descriptor.attribute("s3.bucket.name", bucketName);
    descriptor.attribute("s3.storage.path", storageFolder);
    return descriptor;
  }
  
  public String getStreamKey(StreamDescriptor descriptor) {
    return this.storageFolder + "/stream-" + descriptor.getId();
  }
  
  public StreamDescriptor createStreamDescriptor(int id) {
    StreamDescriptor descriptor = new StreamDescriptor("S3", id, bucketName) ;
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
  
  public S3Source getSource() throws Exception { 
    return new S3Source(getS3Client(), toStorageDesciptor()); 
  }
  
  public S3Source getSource(S3Client s3Client) throws Exception { 
    return new S3Source(getS3Client(), toStorageDesciptor()); 
  }
  
  StorageDescriptor toStorageDesciptor() {
    StorageDescriptor descriptor = new StorageDescriptor("S3") ;
    descriptor.attribute("s3.bucket.name", bucketName);
    descriptor.attribute("s3.storage.path", storageFolder);
    return descriptor ;
 }
  
  void fromStorageDescriptor(StorageDescriptor descriptor) {
    bucketName = descriptor.attribute("s3.bucket.name");
    storageFolder = descriptor.attribute("s3.storage.path");
  }
}