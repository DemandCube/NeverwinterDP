package com.neverwinterdp.scribengin.storage.s3;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;

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
    this.bos        = new ByteArrayOutputStream(4 * 1024 * 1024) ;
    this.objOs      = new ObjectOutputStream(bos);
  }

  public ObjectMetadata getObjectMetadata() { return metadata; }

  public void write(byte[] data) throws IOException {
    objOs.writeInt(data.length);
    objOs.write(data);
  }

  public void waitAndClose(long timeout) throws Exception, IOException, InterruptedException {
    objOs.close();
    byte[] data = bos.toByteArray();
    if(data.length == 0) return;
    ByteArrayInputStream input = new ByteArrayInputStream(data);
    metadata.setContentLength(data.length);
    PutObjectRequest request = new PutObjectRequest(bucketName, key, input, metadata);
    UploadProgressListener uploadListener = new UploadProgressListener();
    request.setGeneralProgressListener(uploadListener);
    request.getRequestClientOptions().setReadLimit(256 * 1024);
    PutObjectResult result = s3Client.getAmazonS3Client().putObject(request);
    uploadListener.waitForUploadComplete(timeout);
    if(uploadListener.getComleteProgressEvent() == null) {
      String mesg = 
          "Cannot get the complete event after " + timeout + "ms\n" + 
          uploadListener.getProgressEventInfo();
      throw new IOException(mesg);
    }
  }
  
  public void forceClose() throws IOException, InterruptedException {
    objOs.close();
  }
  
  static public class UploadProgressListener implements ProgressListener {
    private StringBuilder progressEvents = new StringBuilder();
    private ProgressEvent completeEvent ;
    
    @Override
    synchronized public void progressChanged(ProgressEvent progressEvent) {
      progressEvents.append(progressEvent).append("\n");
      if(progressEvent.getEventType() == ProgressEventType.TRANSFER_COMPLETED_EVENT) {
        completeEvent = progressEvent;
        notifyComplete();
      }
    }
    
    public ProgressEvent getComleteProgressEvent() { return  completeEvent; }
    
    public String getProgressEventInfo() {
      return progressEvents.toString();
    }
    
    synchronized void notifyComplete() {
       notifyAll();
    }
    
    synchronized void waitForUploadComplete(long timeout) throws InterruptedException {
      wait(timeout);;
   }
  }
}
