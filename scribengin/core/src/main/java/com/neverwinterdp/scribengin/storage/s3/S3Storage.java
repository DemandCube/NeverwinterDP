package com.neverwinterdp.scribengin.storage.s3;

import com.neverwinterdp.scribengin.storage.PartitionStreamConfig;
import com.neverwinterdp.scribengin.storage.Storage;
import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.s3.sink.S3Sink;
import com.neverwinterdp.scribengin.storage.s3.source.S3SourcePartition;
import com.neverwinterdp.scribengin.storage.source.Source;

public class S3Storage extends Storage {
  final static public String BUCKET       = "location";
  final static public String FOLDER       = "folder";
  
  private String bucketName ;
  private String storageFolder ;
  
  private S3Client s3Client ;
  //private S3Source s3Source ;

  
  public S3Storage(S3Client s3Client, StorageConfig storageDescriptor) {
    super(storageDescriptor);
    this.s3Client = s3Client;
  }
  
  
  public S3Storage(String bucketName, String storageFolder) {
    super(new StorageConfig("s3", bucketName));
    this.bucketName = bucketName;
    this.storageFolder = storageFolder;
    this.s3Client = new S3Client();
  }
  
  public S3Storage(StorageConfig descriptor) {
    super(new StorageConfig("s3", descriptor.getLocation()));
    fromStorageDescriptor(descriptor);
    this.s3Client = new S3Client();
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
  
  //public S3SourcePartition getSource() throws Exception { 
  //  return new S3SourcePartition(getS3Client(), toStorageDesciptor()); 
  //}
  
  public Source getSource() throws Exception {
    //TODO: Implement S3 source
    return null;
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

  @Override
  public void refresh() throws Exception {
    //TODO: Implement s3Source
    //s3Source = null;
  }

  @Override
  public boolean exists() throws Exception {
    //TODO: check if folder exists too
    return s3Client.hasBucket(this.bucketName);
  }

  @Override
  public void drop() throws Exception {
    s3Client.deleteBucket(this.bucketName, true);
  }

  @Override
  public void create(int numOfPartition, int replication) throws Exception {
    s3Client.createBucket(this.bucketName);
    s3Client.createS3Folder(this.bucketName, this.storageFolder);
  }
}