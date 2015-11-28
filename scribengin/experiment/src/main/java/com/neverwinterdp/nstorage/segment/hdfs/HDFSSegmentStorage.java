package com.neverwinterdp.nstorage.segment.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.nstorage.segment.SegmentDescriptor;
import com.neverwinterdp.nstorage.segment.SegmentRegistry;
import com.neverwinterdp.nstorage.segment.SegmentRegistryPrinter;
import com.neverwinterdp.nstorage.segment.SegmentStorage;
import com.neverwinterdp.nstorage.segment.WriterDescriptor;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.vm.environment.yarn.HDFSUtil;

public class HDFSSegmentStorage extends SegmentStorage {
  private FileSystem fs;
  private String     storageLocation;

  public HDFSSegmentStorage(String clientId, FileSystem fs, String storageLoc, Registry registry, String regPath) throws RegistryException, IOException {
    SegmentRegistry segStorageReg = new SegmentRegistry(registry, regPath);
    if(!registry.exists(regPath)) {
      segStorageReg.initRegistry();
    }
    init(clientId, segStorageReg);
    
    this.fs              = fs;
    this.storageLocation = storageLoc;
    
    Path hdfsStoragePath = new Path(storageLoc);
    if(!fs.exists(hdfsStoragePath)) {
      fs.mkdirs(hdfsStoragePath);
    }
  }

  
  @Override
  protected HDFSSegmentWriter nextSegmentWriter(WriterDescriptor writer, SegmentDescriptor segment) throws RegistryException, IOException {
    return new HDFSSegmentWriter(segRegistry, writer, segment, fs, storageLocation);
  }

  @Override
  public HDFSSegmentConsistencyVerifier getSegmentConsistencyVerifier() {
    return new HDFSSegmentConsistencyVerifier(segRegistry, fs, storageLocation);
  }
  
  public void close() throws RegistryException, IOException {
    if(writer != null) {
      segRegistry.closeWriter(writer);
      writer = null;
    }
  }
  
  public void dump() throws RegistryException, IOException {
    SegmentRegistryPrinter rPrinter = new SegmentRegistryPrinter(System.out, segRegistry);
    rPrinter.print();
    HDFSUtil.dump(fs, storageLocation);
  }
}
