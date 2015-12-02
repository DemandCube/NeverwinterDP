package com.neverwinterdp.scribengin.dataflow.sample;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.storage.s3.S3Client;

public class S3TrackingSamplIntegrationTest  {
  String bucketName = "tracking-sample-integration-test-bucket";
  S3Client s3Client;
  KafkaTrackingSampleRunner trackingSampleRunner = new KafkaTrackingSampleRunner();
  
  @Before
  public void setup() throws Exception {
    s3Client = new S3Client();
    if (!s3Client.hasBucket(bucketName)) {
      s3Client.createBucket(bucketName);
    } else {
      s3Client.deleteS3Folder(bucketName, "aggregate");
    }
    trackingSampleRunner.dataflowMaxRuntime = 60000;
    trackingSampleRunner.setup();
  }
  
  @After
  public void teardown() throws Exception {
    trackingSampleRunner.teardown();
    s3Client.deleteS3Folder(bucketName, "aggregate");
  }
  
  @Test
  public void testTrackingSample() throws Exception {
    trackingSampleRunner.submitVMTMGenrator();
    trackingSampleRunner.submitS3TMDataflow();
    trackingSampleRunner.submitS3VMTMValidator();
    trackingSampleRunner.runMonitor();
  }
}