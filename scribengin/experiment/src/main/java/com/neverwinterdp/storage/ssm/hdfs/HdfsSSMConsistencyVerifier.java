package com.neverwinterdp.storage.ssm.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.storage.ssm.SSMConsistencyVerifier;
import com.neverwinterdp.storage.ssm.SSMRegistry;
import com.neverwinterdp.storage.ssm.SegmentDescriptor;

public class HdfsSSMConsistencyVerifier extends SSMConsistencyVerifier {
  private FileSystem fs;
  private String     storageLocation;
  
  public HdfsSSMConsistencyVerifier(SSMRegistry segReg, FileSystem fs, String storageLoc) {
    super(segReg);
    this.fs = fs;
    this.storageLocation = storageLoc;
  }
  
  @Override
  protected long getDataSegmentLength(SegmentDescriptor segment) throws IOException {
    String segFullPath = storageLocation + "/" + segment.getSegmentId() + ".dat";
    FileStatus fstatus = fs.getFileStatus(new Path(segFullPath));
    return fstatus.getLen();
  }
}
