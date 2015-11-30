package com.neverwinterdp.nstorage.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;

import com.neverwinterdp.nstorage.NStorageReader;
import com.neverwinterdp.nstorage.NStorageRegistry;
import com.neverwinterdp.nstorage.SegmentDescriptor;
import com.neverwinterdp.nstorage.SegmentReadDescriptor;
import com.neverwinterdp.registry.RegistryException;

public class HDFSNStorageReader extends NStorageReader {
  private FileSystem fs;
  private String     storageLocation;
  
  public HDFSNStorageReader(String clientId, NStorageRegistry registry, 
                            FileSystem fs, String storageLoc) throws RegistryException, IOException {
    this.fs              = fs;
    this.storageLocation = storageLoc;
    init(clientId, registry);
  }

  @Override
  protected HDFSSegmentReader createSegmentReader(SegmentDescriptor segment, SegmentReadDescriptor segRead) throws RegistryException, IOException {
    return new HDFSSegmentReader(registry, readerDescriptor, segment, segRead, fs, storageLocation);
  }
}
