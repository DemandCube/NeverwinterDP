package com.neverwinterdp.scribengin.storage.hdfs.sink;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.scribengin.dataflow.DataflowMessage;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.hdfs.Storage;
import com.neverwinterdp.scribengin.storage.sink.SinkStream;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;
import com.neverwinterdp.vm.environment.yarn.HDFSUtil;

public class HDFSSinkStream implements SinkStream {
  private Storage<DataflowMessage> storage;
  private FileSystem fs ;
  private StreamDescriptor descriptor;
  
  
  public HDFSSinkStream(FileSystem fs, Path path) throws IOException {
    this.fs = fs ;
    descriptor = new StreamDescriptor("HDFS", HDFSUtil.getStreamId(path), path.toString());
    storage = new Storage<>(fs, descriptor.getLocation(), DataflowMessage.class);
  }
  
  public HDFSSinkStream(FileSystem fs, StreamDescriptor descriptor) throws IOException {
    this.fs = fs;
    this.descriptor = descriptor;
    storage = new Storage<>(fs, descriptor.getLocation(), DataflowMessage.class);
  }
  
  public StreamDescriptor getPartitionConfig() { return this.descriptor ; }
  
  synchronized public void delete() throws Exception {
  }
  
  @Override
  synchronized public SinkStreamWriter getWriter() throws IOException {
    return new HDFSSinkStreamWriter(storage, descriptor);
  }
  
  public void optimize() throws Exception {
    storage.refresh();
    storage.optimizeBufferSegments();
  }
}