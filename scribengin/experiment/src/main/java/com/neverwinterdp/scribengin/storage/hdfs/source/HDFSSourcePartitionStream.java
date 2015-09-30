package com.neverwinterdp.scribengin.storage.hdfs.source;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;

import com.neverwinterdp.scribengin.storage.PartitionDescriptor;
import com.neverwinterdp.scribengin.storage.source.SourcePartitionStream;
import com.neverwinterdp.scribengin.storage.source.SourcePartitionStreamReader;

public class HDFSSourcePartitionStream implements SourcePartitionStream {
  private FileSystem fs ;
  private PartitionDescriptor descriptor ;
  
  public HDFSSourcePartitionStream(FileSystem fs, PartitionDescriptor descriptor) {
    this.fs = fs;
    this.descriptor = descriptor;
  }
  
  public PartitionDescriptor getDescriptor() { return descriptor ; }
  
  @Override
  public SourcePartitionStreamReader getReader(String name) throws FileNotFoundException, IllegalArgumentException, IOException {
    return new HDFSSourcePartitionStreamReader(name, fs, descriptor) ;
  }

}
