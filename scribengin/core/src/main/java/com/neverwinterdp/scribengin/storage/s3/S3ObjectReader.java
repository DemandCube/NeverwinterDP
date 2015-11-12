package com.neverwinterdp.scribengin.storage.s3;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

import com.neverwinterdp.scribengin.storage.Record;
import com.neverwinterdp.util.JSONSerializer;

public class S3ObjectReader implements Closeable {
  private ObjectInputStream   objIs;
  private byte[] current = null ;
  
  public S3ObjectReader(InputStream inputStream) throws IOException {
    objIs = new ObjectInputStream(new BufferedInputStream(inputStream, 256 * 1024));
  }

  public Record next() {
    return JSONSerializer.INSTANCE.fromBytes(current, Record.class);
  }

  public boolean hasNext() throws IOException {
    if(objIs.available() > 0) {
      int size = objIs.readInt();
      current = new byte[size];
      objIs.read(current);
      return true;
    }
    return false;
  }

  @Override
  public void close() throws IOException {
    objIs.close();
  }
}
