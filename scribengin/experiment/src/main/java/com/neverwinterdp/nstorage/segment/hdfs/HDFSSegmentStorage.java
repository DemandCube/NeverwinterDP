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
  private String     storagePath;
  
  public HDFSSegmentStorage(FileSystem fs, String storagePath, Registry registry, String regPath) throws RegistryException, IOException {
    this.fs          = fs;
    this.storagePath = storagePath;
    Path hdfsStoragePath = new Path(storagePath);
    if(!fs.exists(hdfsStoragePath)) {
      fs.mkdirs(hdfsStoragePath);
    }
    
    SegmentStorageRegistry segStorageReg = new SegmentStorageRegistry(registry, regPath);
    if(!registry.exists(regPath)) {
      segStorageReg.initRegistry();
    }
    init(segStorageReg);
  }
  
  
}
