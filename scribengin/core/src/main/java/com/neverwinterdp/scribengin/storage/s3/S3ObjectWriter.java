package com.neverwinterdp.scribengin.storage.s3;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class S3ObjectWriter {
  private S3Client s3Client;
  private String bucketName;
  private String key;
  private ObjectMetadata metadata ;
  private ByteArrayOutputStream output ;

  public S3ObjectWriter(S3Client s3Client, String bucketName, String key, ObjectMetadata metadata) throws IOException {
    this.s3Client = s3Client;
    this.bucketName = bucketName;
    this.key = key;
    this.metadata = metadata;
    output = new ByteArrayOutputStream() ;
  }

  public ObjectMetadata getObjectMetadata() { return this.metadata; }

  public void write(byte[] data) throws IOException {
    output.write(data);
  }

  public void waitAndClose(long timeout) throws Exception, IOException, InterruptedException {
    System.err.println("Start wait and close") ;
    long start = System.currentTimeMillis() ;
    byte[] data = output.toByteArray();
    output.close(); 
    ByteArrayInputStream input = new ByteArrayInputStream(data);
    metadata.setContentLength(data.length);
    PutObjectRequest request = new PutObjectRequest(bucketName, key, input, metadata);
    request.getRequestClientOptions().setReadLimit(1024); //buffer limit 1M
    s3Client.getAmazonS3Client().putObject(request);
    System.err.println("Finish wait and close in " + (System.currentTimeMillis() - start)) ;
  }
  
  public void forceClose() throws IOException, InterruptedException {
    output.close();
  }
}
