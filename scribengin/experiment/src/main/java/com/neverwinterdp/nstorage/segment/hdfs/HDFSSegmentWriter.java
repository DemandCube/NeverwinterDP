package com.neverwinterdp.nstorage.segment.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.nstorage.segment.SegmentDescriptor;
import com.neverwinterdp.nstorage.segment.SegmentRegistry;
import com.neverwinterdp.nstorage.segment.SegmentWriter;
import com.neverwinterdp.nstorage.segment.WriterDescriptor;
import com.neverwinterdp.registry.RegistryException;

public class HDFSSegmentWriter extends SegmentWriter {
  private FileSystem            fs;
  private String                storageLocation;
  private String                fullPath ;
  private FSDataOutputStream    bufferingOs;
  private int                   bufferNumberOfWrittenRecords;
  private long                  bufferCurrentPosition;
  
  public HDFSSegmentWriter(SegmentRegistry registry, WriterDescriptor writer, SegmentDescriptor segment, 
                           FileSystem fs, String storageLoc) throws RegistryException, IOException {
    super(registry, writer, segment);
    this.fs = fs;
    this.storageLocation = storageLoc;
    this.fullPath = storageLocation + "/" + segment.getName() + ".dat";
    bufferingOs  = fs.create(new Path(fullPath)) ;
  }

  @Override
  protected int  bufferGetNumberOfWrittenRecords() { return this.bufferNumberOfWrittenRecords; }
  
  @Override
  protected long bufferGetCurrentPosistion() { return this.bufferCurrentPosition ; }
  
  
  @Override
  protected boolean isBufferFull() throws IOException, RegistryException {
    return false;
  }

  @Override
  protected void bufferWrite(byte[] data) throws IOException, RegistryException {
    bufferingOs.writeInt(data.length);
    bufferingOs.write(data);
    bufferNumberOfWrittenRecords++ ;
    bufferCurrentPosition += 4 + data.length;
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
    if(bufferingOs != null) {
      bufferingOs.close();
      fs.delete(new Path(fullPath), true);
    }
    bufferingOs  = fs.create(new Path(fullPath)) ;
  }

  @Override
  protected void bufferClose() throws IOException {
    bufferingOs.close();
  }
}