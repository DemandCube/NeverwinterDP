package com.neverwinterdp.nstorage.segment.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.nstorage.segment.DataSegmentDescriptor;
import com.neverwinterdp.nstorage.segment.SegmentDescriptor;
import com.neverwinterdp.nstorage.segment.SegmentStorageRegistry;
import com.neverwinterdp.nstorage.segment.SegmentWriter;
import com.neverwinterdp.registry.RegistryException;

public class HDFSSegmentWriter extends SegmentWriter {
  private FileSystem            fs;
  private String                storageLocation;
  private DataSegmentDescriptor dataSegmentDescriptor;
  private String                relativePath;
  private String                fullPath ;
  private FSDataOutputStream    bufferingOs;
  
  public HDFSSegmentWriter(String name, SegmentStorageRegistry registry, SegmentDescriptor segment, 
                           FileSystem fs, String storageLoc) throws RegistryException, IOException {
    super(name, registry, segment);
    this.fs = fs;
    dataSegmentDescriptor = registry.newDataSegment(name, segment);
    relativePath = segment.getName() + "/" + dataSegmentDescriptor.getName() + ".dat";
    fullPath = storageLocation + "/" + relativePath;
    dataSegmentDescriptor.setLocation(relativePath);
    bufferingOs  = fs.create(new Path(fullPath)) ;
  }

  @Override
  protected boolean isBufferFull() throws IOException, RegistryException {
    return false;
  }

  @Override
  protected void bufferWrite(byte[] data) throws IOException, RegistryException {
    bufferingOs.writeInt(data.length);
    bufferingOs.write(data);
  }

  @Override
  protected void bufferPrepareCommit() throws IOException {
    bufferingOs.hflush();
  }

  @Override
  protected void bufferCompleteCommit() throws IOException {
  }

  @Override
  protected void bufferRollback() throws IOException {
  }
}
