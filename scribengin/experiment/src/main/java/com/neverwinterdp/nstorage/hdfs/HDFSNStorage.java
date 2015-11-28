package com.neverwinterdp.nstorage.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.nstorage.NStorageReaderDescriptor;
import com.neverwinterdp.nstorage.SegmentDescriptor;
import com.neverwinterdp.nstorage.SegmentReader;
import com.neverwinterdp.nstorage.NStorageRegistry;
import com.neverwinterdp.nstorage.NStorageRegistryPrinter;
import com.neverwinterdp.nstorage.NStorage;
import com.neverwinterdp.nstorage.NStorageWriterDescriptor;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.vm.environment.yarn.HDFSUtil;

public class HDFSNStorage extends NStorage {
  private FileSystem fs;
  private String     storageLocation;

  public HDFSNStorage(String clientId, FileSystem fs, String storageLoc, Registry registry, String regPath) throws RegistryException, IOException {
    NStorageRegistry segStorageReg = new NStorageRegistry(registry, regPath);
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
  protected SegmentReader createSegmentReader(NStorageReaderDescriptor reader, SegmentDescriptor segment) throws RegistryException, IOException {
    return null;
  }
  
  @Override
  protected HDFSSegmentWriter createSegmentWriter(NStorageWriterDescriptor writer, SegmentDescriptor segment) throws RegistryException, IOException {
    return new HDFSSegmentWriter(registry, writer, segment, fs, storageLocation);
  }

  @Override
  public HDFSNStorageConsistencyVerifier getSegmentConsistencyVerifier() {
    return new HDFSNStorageConsistencyVerifier(registry, fs, storageLocation);
  }
  
  public void close() throws RegistryException, IOException {
    if(writer != null) {
      registry.closeWriter(writer);
      writer = null;
    }
  }
  
  public void dump() throws RegistryException, IOException {
    NStorageRegistryPrinter rPrinter = new NStorageRegistryPrinter(System.out, registry);
    rPrinter.print();
    HDFSUtil.dump(fs, storageLocation);
  }
}
