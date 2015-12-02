package com.neverwinterdp.ssm.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.ssm.SSMReader;
import com.neverwinterdp.ssm.SSMRegistry;
import com.neverwinterdp.ssm.SegmentDescriptor;
import com.neverwinterdp.ssm.SegmentReadDescriptor;

public class HdfsSSMReader extends SSMReader {
  private FileSystem fs;
  private String     storageLocation;
  
  public HdfsSSMReader(String clientId, SSMRegistry registry, 
                            FileSystem fs, String storageLoc) throws RegistryException, IOException {
    this.fs              = fs;
    this.storageLocation = storageLoc;
    init(clientId, registry);
  }

  @Override
  protected HdfsSegmentReader createSegmentReader(SegmentDescriptor segment, SegmentReadDescriptor segRead) throws RegistryException, IOException {
    return new HdfsSegmentReader(registry, readerDescriptor, segment, segRead, fs, storageLocation);
  }
}
