package com.neverwinterdp.nstorage.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.nstorage.SegmentDescriptor;
import com.neverwinterdp.nstorage.NStorageRegistry;
import com.neverwinterdp.nstorage.SegmentWriter;
import com.neverwinterdp.nstorage.NStorageWriterDescriptor;
import com.neverwinterdp.registry.RegistryException;

public class HDFSSegmentWriter extends SegmentWriter {
  private FileSystem            fs;
  private String                storageLocation;
  private String                segFullPath ;
  private FSDataOutputStream    bufferingOs;
  private long                  bufferNumberOfWrittenRecords;
  private long                  bufferCurrentPosition;
  
  public HDFSSegmentWriter(NStorageRegistry registry, NStorageWriterDescriptor writer, SegmentDescriptor segment, 
                           FileSystem fs, String storageLoc) throws RegistryException, IOException {
    super(registry, writer, segment);
    this.fs = fs;
    this.storageLocation = storageLoc;
    this.segFullPath = storageLocation + "/" + segment.getSegmentId() + ".dat";
    bufferingOs  = fs.create(new Path(segFullPath)) ;
  }

  @Override
  protected long  bufferGetNumberOfWrittenRecords() { return this.bufferNumberOfWrittenRecords; }
  
  @Override
  protected long bufferGetCurrentPosistion() { return this.bufferCurrentPosition ; }
  
  
  @Override
  protected boolean bufferIsFull() throws IOException, RegistryException {
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
      segment.getDataSegmentLastCommitPos();
      bufferingOs.close();
      Path hdfsSegFullPath = new Path(segFullPath);
      fs.truncate(hdfsSegFullPath, segment.getDataSegmentLastCommitPos());
      bufferingOs = fs.append(hdfsSegFullPath);
      bufferNumberOfWrittenRecords = segment.getDataSegmentNumOfRecords();
      bufferCurrentPosition = segment.getDataSegmentLastCommitPos();
    }
  }

  @Override
  protected void bufferClose() throws IOException {
    if(bufferingOs == null) return;
    bufferingOs.close();
    bufferingOs = null;
  }
}