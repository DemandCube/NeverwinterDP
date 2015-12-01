package com.neverwinterdp.nstorage.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.nstorage.NStorageReaderDescriptor;
import com.neverwinterdp.nstorage.NStorageRegistry;
import com.neverwinterdp.nstorage.SegmentDescriptor;
import com.neverwinterdp.nstorage.SegmentReadDescriptor;
import com.neverwinterdp.nstorage.SegmentReader;

public class HDFSSegmentReader extends SegmentReader {
  private FileSystem        fs;
  private String            storageLocation;
  private String            segmentFullPath;
  private FSDataInputStream dataIs;
  private long              currentReadPos = 0;
  
  public HDFSSegmentReader(NStorageRegistry registry, NStorageReaderDescriptor readerDescriptor, 
                           SegmentDescriptor segment, SegmentReadDescriptor segmentReadDescriptor, 
                           FileSystem fs, String storageLoc) throws IllegalArgumentException, IOException {
    super(registry, readerDescriptor, segment, segmentReadDescriptor);
    
    this.fs = fs;
    this.storageLocation = storageLoc;
    
    segmentFullPath = storageLocation + "/" + segment.getSegmentId() + ".dat";
    dataIs  = fs.open(new Path(segmentFullPath)) ;
    if(segmentReadDescriptor.getCommitReadDataPosition() > 0) {
      currentReadPos = segmentReadDescriptor.getCommitReadDataPosition();
      dataIs.seek(currentReadPos);
    }
  }

  @Override
  protected byte[] dataNextRecord() throws IOException {
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
}