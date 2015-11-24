package com.neverwinterdp.nstorage.segment.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.nstorage.segment.SegmentStorage;
import com.neverwinterdp.nstorage.segment.SegmentStorageRegistry;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;

public class HDFSSegmentStorage extends SegmentStorage {
  private FileSystem fs;
  private String     storageLocation;
  
  public HDFSSegmentStorage(FileSystem fs, String storageLoc, Registry registry, String regPath) throws RegistryException, IOException {
    this.fs              = fs;
    this.storageLocation = storageLoc;
    
    Path hdfsStoragePath = new Path(storageLoc);
    if(!fs.exists(hdfsStoragePath)) {
      fs.mkdirs(hdfsStoragePath);
    }
    
    SegmentStorageRegistry segStorageReg = new SegmentStorageRegistry(registry, regPath);
    if(!registry.exists(regPath)) {
      segStorageReg.initRegistry();
    }
    init(segStorageReg);
  }
  
  @Override
  public HDFSStorageWriter getWriter(String name) {
    return new HDFSStorageWriter(name, fs, storageLocation, registry); 
  }
}
