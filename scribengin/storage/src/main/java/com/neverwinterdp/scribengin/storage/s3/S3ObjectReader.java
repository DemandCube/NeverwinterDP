package com.neverwinterdp.scribengin.storage.s3;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;

import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public class S3ObjectReader implements Closeable {
  private S3Object          s3Object;
  S3ObjectInputStream       contentIs;
  private ObjectInputStream objIs;
  private byte[]            current          = null;
  private int               numOfRecords     = 0;
  private int               numOfReadRecords = 0;
  
  private long              contentLength  = 0;
  private long              lastAvailable  = 0;
  private long              accumulateRead = 0;
  
  public S3ObjectReader(S3Object s3Object) throws IOException {
    this.s3Object  = s3Object;
    contentIs = s3Object.getObjectContent();
    objIs = new ObjectInputStream(new BufferedInputStream(contentIs, 1024 * 1024));
    contentLength = s3Object.getObjectMetadata().getContentLength();
    numOfRecords = Integer.parseInt(s3Object.getObjectMetadata().getUserMetaDataOf("num-of-records"));
  }

  public byte[] next() { return current; }
  
  
  public boolean hasNext() throws IOException {
    try {
      lastAvailable = objIs.available();
      if(numOfReadRecords < numOfRecords) {
        int size = objIs.readInt();
        current = new byte[size];
        objIs.readFully(current);
        numOfReadRecords++;
        accumulateRead += 4 + current.length;
        return true;
      }
      return false;
    } catch(Throwable t) {
      dump();
      t.printStackTrace();
      throw t;
    }
  }

  public void dump() throws IOException {
    System.err.println("contentLength: " + contentLength);
    System.err.println("lastAvailable: " + lastAvailable);
    System.err.println("objIs.available(): " + objIs.available());
    System.err.println("accumulateRead: " + accumulateRead);
  }
  
  @Override
  public void close() throws IOException {
    s3Object.close();
    objIs.close();
  }
}
