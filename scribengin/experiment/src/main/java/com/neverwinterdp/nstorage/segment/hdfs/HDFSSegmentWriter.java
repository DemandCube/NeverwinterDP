package com.neverwinterdp.nstorage.segment.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;

import com.neverwinterdp.nstorage.segment.DataSegmentDescriptor;
import com.neverwinterdp.nstorage.segment.SegmentDescriptor;
import com.neverwinterdp.nstorage.segment.SegmentStorageRegistry;
import com.neverwinterdp.nstorage.segment.SegmentWriter;
import com.neverwinterdp.registry.RegistryException;

public class HDFSSegmentWriter extends SegmentWriter {
  private FileSystem fs ;
  private DataSegmentDescriptor dataSegmentDescriptor;
  
  public HDFSSegmentWriter(String name, SegmentStorageRegistry registry, SegmentDescriptor segment, 
                           FileSystem fs) throws RegistryException {
    super(name, registry, segment);
    this.fs = fs;
    dataSegmentDescriptor = registry.newDataSegment(name, segment);
    dataSegmentDescriptor.setLocation(segment.getName() + "/" + dataSegmentDescriptor.getName() + ".dat");
  }

  @Override
  protected boolean isBufferFull() throws IOException, RegistryException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  protected void bufferWrite(byte[] data) throws IOException, RegistryException {
    // TODO Auto-generated method stub
    
  }

  @Override
  protected void bufferPrepareCommit() throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  protected void bufferCompleteCommit() throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  protected void bufferRollback() throws IOException {
    // TODO Auto-generated method stub
    
  }

}
