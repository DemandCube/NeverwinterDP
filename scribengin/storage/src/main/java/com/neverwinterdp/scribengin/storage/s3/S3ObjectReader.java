package com.neverwinterdp.scribengin.storage.s3;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

import com.amazonaws.services.s3.model.S3Object;

public class S3ObjectReader implements Closeable {
  private S3Object          s3Object;
  private ObjectInputStream objIs;
  private byte[]            current        = null;

  public S3ObjectReader(S3Object s3Object) throws IOException {
    this.s3Object  = s3Object;
    InputStream is = s3Object.getObjectContent();
    objIs = new ObjectInputStream(new BufferedInputStream(is, 1024 * 1024));
  }

  public byte[] next() { return current; }
  
  
  public boolean hasNext() throws IOException {
    if(objIs.available() > 0) {
      int size = objIs.readInt();
      current = new byte[size];
      objIs.readFully(current);
      return true;
    }
    return false;
  }

  @Override
  public void close() throws IOException {
    s3Object.close();
    objIs.close();
  }
}
