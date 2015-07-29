package com.neverwinterdp.scribengin.storage.s3;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import com.amazonaws.internal.SdkBufferedInputStream;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class S3ObjectWriter {
  private S3Client s3Client;
  private String bucketName;
  private String key;
  ObjectMetadata metadata = new ObjectMetadata();
  private RecordInputStream input ;
  private Throwable error ;
  private WriteThread writeThread;

  public S3ObjectWriter(S3Client s3Client, String bucketName, String key, ObjectMetadata metadata) throws IOException {
    this.s3Client = s3Client;
    this.bucketName = bucketName;
    this.key = key;
    this.metadata = metadata;

//    pipedOutput = new PipedOutputStream();
//    pipedInput  = new PipedInputStream(pipedOutput, 8 * 1024 * 1024/*buffer size 4M */);
    
    input = new RecordInputStream();
    writeThread = new WriteThread();
    writeThread.start();
  }

  public ObjectMetadata getObjectMetadata() { return this.metadata; }

  public void write(byte[] data) throws IOException {
    try {
      input.write(data);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  public void waitAndClose(long timeout) throws Exception, IOException, InterruptedException {
    System.err.println("Start wait and close") ;
    
    long start = System.currentTimeMillis() ;
    input.end();
    if (!writeThread.waitForTermination(timeout)) {
      throw new IOException("The writer thread cannot upload all the data to S3 in " + timeout + "ms");
    }
    if(error != null) {
      throw new Exception(error) ;
    }
    System.err.println("Finish wait and close in " + (System.currentTimeMillis() - start)) ;
  }
  
  public void forceClose() throws IOException, InterruptedException {
    input.close();
    writeThread.interrupt();
  }

  public class WriteThread extends Thread {
    boolean running = false;
    
    public void run() {
      running = true;
      try {
        PutObjectRequest request = new PutObjectRequest(bucketName, key, input, metadata);
        request.getRequestClientOptions().setReadLimit(1024); //buffer limit 1M
        s3Client.getAmazonS3Client().putObject(request);
      } catch(Throwable t) {
        error = t ;
      }
      running = false;
      notifyTermination();
    }

    synchronized void notifyTermination() {
      notify();
    }

    synchronized boolean waitForTermination(long timeout) throws InterruptedException {
      wait(timeout);
      return !running;
    }
  }
}
