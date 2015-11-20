package com.neverwinterdp.nstorage.segment.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;

import com.neverwinterdp.nstorage.segment.SegmentDescriptor;
import com.neverwinterdp.nstorage.segment.SegmentStorageRegistry;
import com.neverwinterdp.nstorage.segment.SegmentWriter;
import com.neverwinterdp.nstorage.segment.SegmentStorageWriter;
import com.neverwinterdp.registry.RegistryException;

public class HDFSStorageWriter extends SegmentStorageWriter {
  private FileSystem             fs ;
  
  public HDFSStorageWriter(FileSystem fs, SegmentStorageRegistry segStorageReg) {
    super(segStorageReg);
    this.fs            = fs;
  }

  @Override
  protected SegmentWriter nextSegmentWriter(SegmentDescriptor segment) {
    return null;
  }
}
