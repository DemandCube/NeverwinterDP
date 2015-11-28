package com.neverwinterdp.nstorage.segment.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.nstorage.segment.SegmentDescriptor;
import com.neverwinterdp.nstorage.segment.SegmentReader;
import com.neverwinterdp.nstorage.segment.SegmentRegistry;

public class HDFSSegmentReader extends SegmentReader {
  private FileSystem        fs;
  private String            storageLocation;
  private String            fullPath;
  private FSDataInputStream dataIs;
  
  public HDFSSegmentReader(SegmentRegistry segStorageReg, SegmentDescriptor segment, FileSystem fs, String storageLoc) throws IllegalArgumentException, IOException {
    super(segStorageReg, segment);
    this.fs = fs;
    this.storageLocation = storageLoc;
    fullPath = storageLocation + "/" + segment.getName() + ".dat";
    dataIs  = fs.open(new Path(fullPath)) ;
  }

  @Override
  protected byte[] dataNextRecord() throws IOException {
    int size    = dataIs.readInt();
    byte[] data = new byte[size];
    dataIs.readFully(data);
    return data;
  }

}
