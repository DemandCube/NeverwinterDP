package com.neverwinterdp.scribengin.storage.hdfs.sink;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.scribengin.dataflow.DataflowMessage;
import com.neverwinterdp.scribengin.storage.hdfs.segment.Segment;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;
import com.neverwinterdp.util.JSONSerializer;

public class HDFSSinkStreamWriter implements SinkStreamWriter {
  private FileSystem fs;
  private String location ;
  private SinkBuffer currentBuffer ;
  
  public HDFSSinkStreamWriter(FileSystem fs, String location) throws IOException {
    this.fs = fs;
    this.location = location;
    if(!fs.exists(new Path(location))) fs.mkdirs(new Path(location));
  }
  
  @Override
  public void append(DataflowMessage dataflowMessage) throws Exception {
    if(currentBuffer == null) {
      currentBuffer = nextSinkBuffer();
    }
    currentBuffer.append(dataflowMessage);
  }

  @Override
  public void prepareCommit() throws Exception {
    //TODO: reimplement correctly 2 phases commit
  }

  @Override
  public void completeCommit() throws Exception {
    if(currentBuffer == null) return;
    //TODO: reimplement correctly 2 phases commit
    currentBuffer.commit();
    currentBuffer = null ;
  }
  
  @Override
  synchronized public void commit() throws Exception {
    prepareCommit();
    completeCommit();
  }

  @Override
  public void rollback() throws Exception {
    currentBuffer.rollback();
    currentBuffer = null ;
  }
  
  @Override
  synchronized public void close() throws Exception {
    if(currentBuffer != null) {
      currentBuffer.rollback();
      currentBuffer = null;
    }
  }
  
  private SinkBuffer nextSinkBuffer() throws IOException {
    SinkBuffer buffer = new SinkBuffer() ;
    return buffer;
  }
  
  public String toString() {
    StringBuilder b = new StringBuilder() ;
    b.append("location=").append(location);
    return b.toString() ;
  }
  
  class SinkBuffer {
    private Path bufferingPath;
    private Path dataPath;
    private Segment segment;
    private FSDataOutputStream output;
    private int  count = 0 ;
    
    public SinkBuffer() throws IOException {
      segment = new Segment();
      bufferingPath  = new Path(segment.toBufferingPath(location)) ;
      dataPath       = new Path(segment.toDataPath(location)) ;
      output         = fs.create(bufferingPath) ;
    }
    
    public void append(DataflowMessage dataflowMessage) throws IOException {
      byte[] bytes = JSONSerializer.INSTANCE.toBytes(dataflowMessage) ;
      output.writeInt(bytes.length);
      output.write(bytes);
      count++;
    }
    
    public void rollback() throws IOException {
      output.close();
      fs.delete(bufferingPath, true) ;
      count = 0 ;
    }
    
    public void commit() throws IOException {
      output.close();
      if(count <= 0) {
        fs.delete(bufferingPath, true) ;
      } else {
        fs.rename(bufferingPath, dataPath);
      }
    }
  }
}