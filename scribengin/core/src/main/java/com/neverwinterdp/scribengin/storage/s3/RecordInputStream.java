package com.neverwinterdp.scribengin.storage.s3;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.LinkedBlockingQueue;

public class RecordInputStream extends InputStream {
  private LinkedBlockingQueue<Record> recordHolder = new LinkedBlockingQueue<Record>();
  private Record       currentRecord ;
  private boolean      end = false;
  
  public void write(byte[] data) throws InterruptedException {
    recordHolder.put(new Record(data)) ;
  }
  
  @Override
  public int read() throws IOException {
    if(currentRecord == null) {
      if(end && recordHolder.size() == 0) return -1;
      try {
        currentRecord = recordHolder.take() ;
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
    int ret = currentRecord.read() ;
    if(ret == -1) {
      currentRecord = null ;
      return read() ;
    }
    return ret ;
  }
  
  public void end() { end = true ; }

  static public class Record {
    byte[] data ;
    int    readIdx ;
    
    public Record(byte[] data) {
      this.data = data ;
    }
    
    public int read() {
      if(readIdx >= data.length) return -1 ;
      return data[readIdx++] ;
    }
  }
}
