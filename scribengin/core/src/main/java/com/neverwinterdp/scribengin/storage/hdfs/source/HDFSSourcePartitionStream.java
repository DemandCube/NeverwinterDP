package com.neverwinterdp.scribengin.storage.hdfs.source;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;

import com.neverwinterdp.scribengin.storage.PartitionStreamConfig;
import com.neverwinterdp.scribengin.storage.source.SourcePartitionStream;

public class HDFSSourcePartitionStream implements SourcePartitionStream {
  private FileSystem fs ;
  private PartitionStreamConfig descriptor ;
  
  public HDFSSourcePartitionStream(FileSystem fs, PartitionStreamConfig descriptor) {
    this.fs = fs;
    this.descriptor = descriptor;
  }
  
  public PartitionStreamConfig getDescriptor() { return descriptor ; }
  
  @Override
  public  HDFSSourcePartitionStreamReader getReader(String name) throws FileNotFoundException, IllegalArgumentException, IOException {
    return new HDFSSourcePartitionStreamReader(name, fs, descriptor) ;
  }

}
