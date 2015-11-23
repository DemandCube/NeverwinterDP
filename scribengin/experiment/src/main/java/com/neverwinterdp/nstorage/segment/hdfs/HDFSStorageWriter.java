package com.neverwinterdp.nstorage.segment.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;

import com.neverwinterdp.nstorage.segment.SegmentDescriptor;
import com.neverwinterdp.nstorage.segment.SegmentStorageRegistry;
import com.neverwinterdp.nstorage.segment.SegmentStorageWriter;
import com.neverwinterdp.nstorage.segment.SegmentWriter;
import com.neverwinterdp.registry.RegistryException;

public class HDFSStorageWriter extends SegmentStorageWriter {
  private String     name;
  private FileSystem fs;
  private String     storageLocation;
  
  public HDFSStorageWriter(String name, FileSystem fs, String storageLocation, SegmentStorageRegistry segStorageReg) {
    super(segStorageReg);
    this.fs              = fs;
    this.storageLocation = storageLocation;
  }

  @Override
  protected SegmentWriter nextSegmentWriter(SegmentDescriptor segment) throws RegistryException, IOException {
    return new HDFSSegmentWriter(name, segStorageReg, segment, fs, storageLocation);
  }
}
