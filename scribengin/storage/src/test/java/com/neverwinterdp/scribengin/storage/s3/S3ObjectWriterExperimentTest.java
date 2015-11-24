package com.neverwinterdp.scribengin.storage.s3;

import java.io.IOException;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.services.s3.model.ObjectMetadata;

public class S3ObjectWriterExperimentTest {
  static public String BUCKET_NAME = "s3-client-integration-test-" + UUID.randomUUID();
  
  static S3Client s3Client ;
  
  @BeforeClass
  static public void beforeClass() {
    s3Client = new S3Client() ;
    s3Client.createBucket(BUCKET_NAME);
  }
  
  @AfterClass
  static public void afterClass() {
    s3Client.deleteBucket(BUCKET_NAME, true);
    s3Client.onDestroy();
  }
  
  @Test
  public void testS3ObjectWriter() throws Exception, IOException, InterruptedException {
    String KEY = "test-s3-object-writer" ;
    S3ObjectWriter writer = new S3ObjectWriter(s3Client, BUCKET_NAME, KEY, new ObjectMetadata());
    int totalBytes = 0 ;
    for(int i = 0; i < 100000; i++) {
      byte[] data = new byte[512] ;
      writer.write(data);
      totalBytes += data.length;
      if((i + 1) % 1000 == 0) {
        System.out.println("write " + i + ", bytes " + totalBytes); 
      }
    }
    writer.waitAndClose(5000);
  }
}