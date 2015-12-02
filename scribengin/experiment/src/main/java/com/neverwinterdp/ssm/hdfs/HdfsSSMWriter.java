package com.neverwinterdp.ssm.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.ssm.SSMRegistry;
import com.neverwinterdp.ssm.SSMWriter;
import com.neverwinterdp.ssm.SSMWriterDescriptor;
import com.neverwinterdp.ssm.SegmentDescriptor;

public class HdfsSSMWriter extends SSMWriter {
  private FileSystem fs;
  private String     storageLocation;
  
  public HdfsSSMWriter(String clientId, SSMRegistry registry, FileSystem fs, String storageLoc) throws RegistryException {
    super(clientId, registry);
    this.fs              = fs;
    this.storageLocation = storageLoc;
  }

  @Override
  protected HdfsSegmentWriter createSegmentWriter(SSMWriterDescriptor writer, SegmentDescriptor segment) throws RegistryException, IOException {
    return new HdfsSegmentWriter(registry, writer, segment, fs, storageLocation);
  }
}
