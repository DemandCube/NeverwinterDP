package com.neverwinterdp.nstorage.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.nstorage.NStorageConsistencyVerifier;
import com.neverwinterdp.nstorage.SegmentDescriptor;
import com.neverwinterdp.nstorage.NStorageRegistry;

public class HDFSNStorageConsistencyVerifier extends NStorageConsistencyVerifier {
  private FileSystem fs;
  private String     storageLocation;
  
  public HDFSNStorageConsistencyVerifier(NStorageRegistry segReg, FileSystem fs, String storageLoc) {
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
