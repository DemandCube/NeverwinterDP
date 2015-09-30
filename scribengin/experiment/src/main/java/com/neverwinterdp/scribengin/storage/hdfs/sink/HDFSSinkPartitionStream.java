package com.neverwinterdp.scribengin.storage.hdfs.sink;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.scribengin.storage.Record;
import com.neverwinterdp.scribengin.storage.PartitionDescriptor;
import com.neverwinterdp.scribengin.storage.hdfs.Storage;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStream;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStreamWriter;
import com.neverwinterdp.vm.environment.yarn.HDFSUtil;

public class HDFSSinkPartitionStream implements SinkPartitionStream {
  private Storage<Record> storage;
  private FileSystem fs ;
  private PartitionDescriptor descriptor;
  
  
  public HDFSSinkPartitionStream(FileSystem fs, Path path) throws IOException {
    this.fs = fs ;
    descriptor = new PartitionDescriptor("HDFS", HDFSUtil.getStreamId(path), path.toString());
    storage = new Storage<>(fs, descriptor.getLocation(), Record.class);
  }
  
  public HDFSSinkPartitionStream(FileSystem fs, PartitionDescriptor descriptor) throws IOException {
    this.fs = fs;
    this.descriptor = descriptor;
    storage = new Storage<>(fs, descriptor.getLocation(), Record.class);
  }
  
  public PartitionDescriptor getDescriptor() { return this.descriptor ; }
  
  synchronized public void delete() throws Exception {
  }
  
  @Override
  synchronized public SinkPartitionStreamWriter getWriter() throws IOException {
    return new HDFSSinkPartitionStreamWriter(storage, descriptor);
  }
  
  public void optimize() throws Exception {
    storage.refresh();
    storage.optimizeBufferSegments();
  }
}