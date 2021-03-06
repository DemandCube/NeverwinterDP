package com.neverwinterdp.storage.simplehdfs.source;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;

import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.source.SourcePartitionStream;

public class HDFSSourcePartitionStream implements SourcePartitionStream {
  private FileSystem fs ;
  private PartitionStreamConfig descriptor ;
  
  public HDFSSourcePartitionStream(FileSystem fs, PartitionStreamConfig descriptor) {
    this.fs = fs;
    this.descriptor = descriptor;
  }
  
  public PartitionStreamConfig getPartitionStreamConfig() { return descriptor ; }
  
  @Override
  public  HDFSSourcePartitionStreamReader getReader(String name) throws FileNotFoundException, IllegalArgumentException, IOException {
    return new HDFSSourcePartitionStreamReader(name, fs, descriptor) ;
  }

}
