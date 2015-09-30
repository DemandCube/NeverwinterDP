package com.neverwinterdp.scribengin.storage;

import org.apache.hadoop.fs.FileSystem;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.scribengin.storage.kafka.KafkaStorage;
import com.neverwinterdp.scribengin.storage.s3.S3Client;

@Singleton
public class StorageService {
  @Inject
  private FileSystem fs;
  
  @Inject
  private S3Client s3Client;
  
  public StorageService() {
  }
  
  public StorageService(FileSystem fs, S3Client s3Client) {
    this.fs = fs;
    this.s3Client = s3Client;
  }
  
  public FileSystem getFileSyztem() { return this.fs ; }

  public Storage getStorage(StorageDescriptor descriptor) throws Exception {
    if("kafka".equalsIgnoreCase(descriptor.getType())) {
      return new KafkaStorage(descriptor);
    }
    throw new Exception("Unknown sink type " + descriptor.getType());
  }
}