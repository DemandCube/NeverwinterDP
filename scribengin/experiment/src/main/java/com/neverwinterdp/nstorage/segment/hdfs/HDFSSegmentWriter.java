package com.neverwinterdp.nstorage.segment.hdfs;

import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.nstorage.segment.SegmentDescriptor;
import com.neverwinterdp.nstorage.segment.SegmentRegistry;
import com.neverwinterdp.nstorage.segment.SegmentWriter;
import com.neverwinterdp.nstorage.segment.WriterDescriptor;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.vm.environment.yarn.HDFSUtil;

public class HDFSSegmentWriter extends SegmentWriter {
  private FileSystem            fs;
  private String                storageLocation;
  private String                segFullPath ;
  private FSDataOutputStream    bufferingOs;
  private long                  bufferNumberOfWrittenRecords;
  private long                  bufferCurrentPosition;
  
  public HDFSSegmentWriter(SegmentRegistry registry, WriterDescriptor writer, SegmentDescriptor segment, 
                           FileSystem fs, String storageLoc) throws RegistryException, IOException {
    super(registry, writer, segment);
    this.fs = fs;
    this.storageLocation = storageLoc;
    this.segFullPath = storageLocation + "/" + segment.getName() + ".dat";
    bufferingOs  = fs.create(new Path(segFullPath)) ;
  }

  @Override
  protected long  bufferGetNumberOfWrittenRecords() { return this.bufferNumberOfWrittenRecords; }
  
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
      segment.getDataSegmentLastCommitPos();
      bufferingOs.close();
      Path hdfsSegFullPath = new Path(segFullPath);
      HDFSUtil.truncate(fs, hdfsSegFullPath, segment.getDataSegmentLastCommitPos());
      if(fs instanceof LocalFileSystem) {
        FileOutputStream fout = new FileOutputStream(segFullPath, true);
        Statistics statistics = fs.getStatistics("file",  LocalFileSystem.class);
        bufferingOs = new  FSDataOutputStream(fout, statistics);
      } else {
        bufferingOs = fs.append(hdfsSegFullPath);
      }
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