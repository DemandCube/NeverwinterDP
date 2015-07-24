package com.neverwinterdp.scribengin.storage.s3;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import com.amazonaws.internal.SdkBufferedInputStream;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
//import org.jets3t.service.io.RepeatableInputStream;

public class S3ObjectWriter {
  private S3Client s3Client;
  private String bucketName;
  private String key;
  ObjectMetadata metadata = new ObjectMetadata();
  private PipedOutputStream pipedOutput;
  private PipedInputStream pipedInput;
  private SdkBufferedInputStream bufferedPipedInput;
  private Throwable error ;
  
  private WriteThread writeThread;

  public S3ObjectWriter(S3Client s3Client, String bucketName, String key, ObjectMetadata metadata) throws IOException {
    this.s3Client = s3Client;
    this.bucketName = bucketName;
    this.key = key;
    this.metadata = metadata;
    
    pipedOutput = new PipedOutputStream();
    pipedInput  = new PipedInputStream(pipedOutput,  4 * 1024 * 1024/*buffer size 4M */);
    bufferedPipedInput = new SdkBufferedInputStream(pipedInput, 512 * 1024/*buffer size 2M */) ;
    writeThread = new WriteThread();
    writeThread.start();
  }

  public ObjectMetadata getObjectMetadata() { return this.metadata; }

  public void write(byte[] data) throws IOException {
    pipedOutput.write(data);
  }

  public void waitAndClose(long timeout) throws IOException, InterruptedException {
    pipedOutput.close();
    if (!writeThread.waitForTermination(timeout)) {
      throw new IOException("The writer thread cannot upload all the data to S3 in " + timeout + "ms");
    }
    pipedInput.close();
    bufferedPipedInput.close();
    if(error != null) {
      throw new RuntimeException(error) ;
    }
  }
  
  public void forceClose() throws IOException, InterruptedException {
    pipedOutput.close();
    writeThread.interrupt();
    pipedInput.close();
    bufferedPipedInput.close();
  }

  public class WriteThread extends Thread {
    boolean running = false;
    
    public void run() {
      running = true;
      try {
        PutObjectRequest request = new PutObjectRequest(bucketName, key, bufferedPipedInput, metadata);
        request.getRequestClientOptions().setReadLimit(1 * 1024 * 1024); //buffer limit 1M
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
