package com.neverwinterdp.scribengin.storage.s3;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class S3ObjectWriter {
  private S3Client s3Client;
  private String bucketName;
  private String key;
  private ObjectMetadata metadata ;
  private ByteArrayOutputStream bos ;
  private ObjectOutputStream    objOs;
  
  public S3ObjectWriter(S3Client s3Client, String bucketName, String key, ObjectMetadata metadata) throws IOException {
    this.s3Client   = s3Client;
    this.bucketName = bucketName;
    this.key        = key;
    this.metadata   = metadata;
    this.bos        = new ByteArrayOutputStream() ;
    this.objOs      = new ObjectOutputStream(bos);
  }

  public ObjectMetadata getObjectMetadata() { return this.metadata; }

  public void write(byte[] data) throws IOException {
    objOs.writeInt(data.length);
    objOs.write(data);
  }

  public void waitAndClose(long timeout) throws Exception, IOException, InterruptedException {
    objOs.close();
    byte[] data = bos.toByteArray();
    bos.close();
    ByteArrayInputStream input = new ByteArrayInputStream(data);
    metadata.setContentLength(data.length);
    PutObjectRequest request = new PutObjectRequest(bucketName, key, input, metadata);
    request.getRequestClientOptions().setReadLimit(1024); //buffer limit 1M
    s3Client.getAmazonS3Client().putObject(request);
  }
  
  public void forceClose() throws IOException, InterruptedException {
    bos.close();
  }
}
