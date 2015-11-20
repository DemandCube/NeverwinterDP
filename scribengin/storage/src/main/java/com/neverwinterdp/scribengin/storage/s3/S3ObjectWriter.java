package com.neverwinterdp.scribengin.storage.s3;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
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
    request.getRequestClientOptions().setReadLimit(256 * 1024);
    UploadProgressListener uploadListener = new UploadProgressListener();
    request.setGeneralProgressListener(uploadListener);
    try {
      PutObjectResult result = s3Client.getAmazonS3Client().putObject(request);
      uploadListener.waitForUploadComplete(timeout);
      if(uploadListener.getComleteProgressEvent() == null) {
        String mesg = 
            "Cannot get the complete event after " + timeout + "ms\n" + 
            uploadListener.getProgressEventInfo();
        throw new IOException(mesg);
      }
    } catch (AmazonServiceException ase) {
      System.out.println("Error Message:    " + ase.getMessage());
      System.out.println("HTTP Status Code: " + ase.getStatusCode());
      System.out.println("AWS Error Code:   " + ase.getErrorCode());
      System.out.println("Error Type:       " + ase.getErrorType());
      System.out.println("Request ID:       " + ase.getRequestId());
      throw new IOException(ase);
    } catch (AmazonClientException ace) {
      System.out.println("Error Message: " + ace.getMessage());
      throw new IOException(ace);
    } 
  }
  
  public void forceClose() throws IOException, InterruptedException {
    objOs.close();
  }
  
  static public class UploadProgressListener implements ProgressListener {
    private int requestByteTransferEvent = 0;
    private ProgressEvent lastRequestByteTransferEvent  ;
    private StringBuilder progressEvents = new StringBuilder();
    private ProgressEvent completeEvent ;
    
    @Override
    synchronized public void progressChanged(ProgressEvent progressEvent) {
      if(progressEvent.getEventType() == ProgressEventType.REQUEST_BYTE_TRANSFER_EVENT) {
        requestByteTransferEvent++;
        lastRequestByteTransferEvent = progressEvent;
      } else {
        progressEvents.append(progressEvent).append("\n");
      }
      if(progressEvent.getEventType() == ProgressEventType.TRANSFER_COMPLETED_EVENT) {
        completeEvent = progressEvent;
        notifyComplete();
      }
    }
    
    public ProgressEvent getComleteProgressEvent() { return  completeEvent; }
    
    public String getProgressEventInfo() {
      String info = 
          "lastRequestByteTransferEvent = " + lastRequestByteTransferEvent + "\n" +
          "REQUEST_BYTE_TRANSFER_EVENT = " + requestByteTransferEvent + "\n" +
          progressEvents.toString();
      return info;
    }
    
    synchronized void notifyComplete() {
       notifyAll();
    }
    
    synchronized void waitForUploadComplete(long timeout) throws InterruptedException {
      if(completeEvent != null) return;
      wait(timeout);
   }
  }
}
