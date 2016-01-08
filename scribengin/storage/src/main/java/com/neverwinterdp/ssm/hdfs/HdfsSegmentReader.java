package com.neverwinterdp.ssm.hdfs;

import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.ssm.SSMReaderDescriptor;
import com.neverwinterdp.ssm.SSMRegistry;
import com.neverwinterdp.ssm.SegmentDescriptor;
import com.neverwinterdp.ssm.SegmentReadDescriptor;
import com.neverwinterdp.ssm.SegmentReader;

public class HdfsSegmentReader extends SegmentReader {
  private FileSystem        fs;
  private String            storageLocation;
  private String            segmentFullPath;
  private FSDataInputStream dataIs;
  private long              currentReadPos = 0;
  
  public HdfsSegmentReader(SSMRegistry registry, SSMReaderDescriptor readerDescriptor, 
                           SegmentDescriptor segment, SegmentReadDescriptor segmentReadDescriptor, 
                           FileSystem fs, String storageLoc) throws IllegalArgumentException, IOException {
    super(registry, readerDescriptor, segment, segmentReadDescriptor);
    
    this.fs = fs;
    this.storageLocation = storageLoc;
    
    segmentFullPath = storageLocation + "/" + segment.getSegmentId() + ".dat";
    
    
    if(segmentReadDescriptor.getCommitReadDataPosition() > 0) {
      currentReadPos = segmentReadDescriptor.getCommitReadDataPosition();
    }
  }

  void openDataInputStream() throws IOException {
    dataIs  = fs.open(new Path(segmentFullPath)) ;
    if(currentReadPos > 0) dataIs.seek(currentReadPos);
  }
  
  @Override
  protected byte[] dataNextRecord() throws IOException {
    try {
      if(dataIs == null) openDataInputStream();
      return dataNextRecordWithRetry();
    } catch(IOException ex) {
      System.err.println("dataNextRecord() currentReadPos = " + currentReadPos + ", commit pos = " + segment.getDataSegmentLastCommitPos());
      ex.printStackTrace();
      throw ex;
    }
  }

  byte[] dataNextRecordWithRetry() throws IOException {
    try {
      int size    = dataIs.readInt();
      byte[] data = new byte[size];
      dataIs.readFully(data);
      currentReadPos += 4 + data.length;
      return data;
    } catch(EOFException ex) {
      dataIs.close();
      openDataInputStream();
    }
    int size    = dataIs.readInt();
    byte[] data = new byte[size];
    dataIs.readFully(data);
    currentReadPos += 4 + data.length;
    return data;
  }
  
  protected void rollback(long readRecordIndex, long pos) throws IOException {
    dataIs.seek(pos);
    currentReadPos = pos;
  }
  
  @Override
  protected long getCurrentReadPosition() { return currentReadPos; }
  
  protected void doClose() throws IOException {
    dataIs.close();
  }
}