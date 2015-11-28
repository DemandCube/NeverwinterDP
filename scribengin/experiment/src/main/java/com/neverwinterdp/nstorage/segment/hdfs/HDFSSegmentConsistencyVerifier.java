package com.neverwinterdp.nstorage.segment.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.nstorage.segment.SegmentConsistencyVerifier;
import com.neverwinterdp.nstorage.segment.SegmentDescriptor;
import com.neverwinterdp.nstorage.segment.SegmentRegistry;

public class HDFSSegmentConsistencyVerifier extends SegmentConsistencyVerifier {
  private FileSystem fs;
  private String     storageLocation;
  
  public HDFSSegmentConsistencyVerifier(SegmentRegistry segReg, FileSystem fs, String storageLoc) {
    super(segReg);
    this.fs = fs;
    this.storageLocation = storageLoc;
  }
  
  @Override
  protected long getDataSegmentLength(SegmentDescriptor segment) throws IOException {
    String segFullPath = storageLocation + "/" + segment.getName() + ".dat";
    FileStatus fstatus = fs.getFileStatus(new Path(segFullPath));
    return fstatus.getLen();
  }
}
