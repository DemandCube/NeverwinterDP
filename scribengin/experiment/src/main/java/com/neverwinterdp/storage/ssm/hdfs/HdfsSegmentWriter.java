package com.neverwinterdp.storage.ssm.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.storage.ssm.SSMRegistry;
import com.neverwinterdp.storage.ssm.SSMWriterDescriptor;
import com.neverwinterdp.storage.ssm.SegmentDescriptor;
import com.neverwinterdp.storage.ssm.SegmentWriter;

public class HdfsSegmentWriter extends SegmentWriter {
  private FileSystem            fs;
  private String                storageLocation;
  private String                segFullPath ;
  private FSDataOutputStream    bufferingOs;
  private long                  currentSegmentSize;
  private long                  uncommitBufferSize;
  
  public HdfsSegmentWriter(SSMRegistry registry, SSMWriterDescriptor writer, SegmentDescriptor segment, 
                           FileSystem fs, String storageLoc) throws RegistryException, IOException {
    super(registry, writer, segment);
    this.fs = fs;
    this.storageLocation = storageLoc;
    this.segFullPath = storageLocation + "/" + segment.getSegmentId() + ".dat";
    bufferingOs  = fs.create(new Path(segFullPath)) ;
  }

  @Override
  protected long bufferGetSegmentSize() { return currentSegmentSize ; }

  @Override
  protected long bufferGetUncommitSize() { return uncommitBufferSize; }
  
  @Override
  protected void bufferWrite(byte[] data) throws IOException, RegistryException {
    bufferingOs.writeInt(data.length);
    bufferingOs.write(data);
    currentSegmentSize += 4 + data.length;
    uncommitBufferSize += 4 + data.length;
  }

  @Override
  protected void bufferPrepareCommit() throws IOException {
    bufferingOs.hflush();
  }

  @Override
  protected void bufferCompleteCommit() throws IOException {
    uncommitBufferSize = 0;
  }

  @Override
  protected void bufferRollback() throws IOException {
    if(bufferingOs != null) {
      segment.getDataSegmentLastCommitPos();
      bufferingOs.close();
      Path hdfsSegFullPath = new Path(segFullPath);
      fs.truncate(hdfsSegFullPath, segment.getDataSegmentLastCommitPos());
      bufferingOs = fs.append(hdfsSegFullPath);
      currentSegmentSize = segment.getDataSegmentLastCommitPos();
      uncommitBufferSize = 0;
    }
  }

  @Override
  protected void bufferClose() throws IOException {
    if(bufferingOs == null) return;
    bufferingOs.close();
    bufferingOs = null;
  }
}