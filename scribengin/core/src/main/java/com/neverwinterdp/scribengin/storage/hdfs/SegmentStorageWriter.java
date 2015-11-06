package com.neverwinterdp.scribengin.storage.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.util.JSONSerializer;

public class SegmentStorageWriter<T> {
  private SegmentStorage<T> storage;
  private FileSystem        fs;
  private String            location;
  private SegmentWriter     currentBuffer;
  
  public SegmentStorageWriter(SegmentStorage<T> storage) {
    this.storage = storage ;
    this.fs = storage.getFileSystem();
    this.location = storage.getLocation();
  }
  
  public SegmentStorage<T> getStorage() { return this.storage; }
  
  public void append(T obj) throws Exception {
    if(currentBuffer == null) {
      currentBuffer = nextSegmentWriter();
    }
    currentBuffer.append(obj);
  }

  public void prepareCommit() throws Exception {
  }

  public void completeCommit() throws Exception {
    if(currentBuffer == null) return;
    //TODO: reimplement correctly 2 phases commit
    currentBuffer.commit();
    currentBuffer = null ;
  }
  
  synchronized public void commit() throws Exception {
    prepareCommit();
    completeCommit();
  }

  public void rollback() throws Exception {
    currentBuffer.rollback();
    currentBuffer = null ;
  }
  
  synchronized public void close() throws Exception {
    if(currentBuffer != null) {
      currentBuffer.rollback();
      currentBuffer = null;
    }
  }
  
  private SegmentWriter nextSegmentWriter() throws IOException {
    SegmentWriter buffer = new SegmentWriter() ;
    return buffer;
  }
  
  public String toString() {
    StringBuilder b = new StringBuilder() ;
    b.append("location=").append(location);
    return b.toString() ;
  }
  
  class SegmentWriter {
    private Path bufferingPath;
    private Path dataPath;
    private Segment segment;
    private FSDataOutputStream bufferingOs;
    private int  count = 0 ;
    
    public SegmentWriter() throws IOException {
      segment        = new Segment();
      bufferingPath  = new Path(segment.toBufferingPath(location)) ;
      dataPath       = new Path(segment.toDataPath(location)) ;
      bufferingOs    = fs.create(bufferingPath) ;
    }
    
    public void append(T obj) throws IOException {
      byte[] bytes = JSONSerializer.INSTANCE.toBytes(obj) ;
      bufferingOs.writeInt(bytes.length);
      bufferingOs.write(bytes);
      count++;
    }
    
    public void rollback() throws IOException {
      bufferingOs.close();
      fs.delete(bufferingPath, true) ;
      count = 0 ;
    }
    
    public void commit() throws IOException {
      if(count <= 0) {
        bufferingOs.close();
        boolean deleted = fs.delete(bufferingPath, true) ;
        if(!deleted) {
          throw new IOException("Cannot delete empty path " + bufferingPath);
        }
      } else {
        bufferingOs.hflush();
        bufferingOs.close();
        fs.rename(bufferingPath, dataPath);
      }
    }
  }
}