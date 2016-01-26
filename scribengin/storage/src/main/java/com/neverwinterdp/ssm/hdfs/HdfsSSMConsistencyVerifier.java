package com.neverwinterdp.ssm.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.ssm.SSMConsistencyVerifier;
import com.neverwinterdp.ssm.SSMRegistry;
import com.neverwinterdp.ssm.SegmentDescriptor;

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