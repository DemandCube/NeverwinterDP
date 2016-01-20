package com.neverwinterdp.ssm.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.ssm.SSM;
import com.neverwinterdp.ssm.SSMReader;
import com.neverwinterdp.ssm.SSMRegistry;
import com.neverwinterdp.ssm.SSMRegistryPrinter;
import com.neverwinterdp.ssm.SSMWriter;
import com.neverwinterdp.vm.environment.yarn.HDFSUtil;

public class HdfsSSM extends SSM {
  private FileSystem fs;
  private String     storageLocation;

  public HdfsSSM(FileSystem fs, String storageLoc, Registry registry, String regPath) throws RegistryException, IOException {
    this.fs              = fs;
    this.storageLocation = storageLoc;
    
    SSMRegistry segStorageReg = new SSMRegistry(registry, regPath);
    if(!registry.exists(regPath)) {
      segStorageReg.initRegistry();
    }
    init(segStorageReg);
    
    Path hdfsStoragePath = new Path(storageLoc);
    if(!fs.exists(hdfsStoragePath)) {
      fs.mkdirs(hdfsStoragePath);
    }
  }
  
  public HdfsSSM(FileSystem fs, String storageLoc, SSMRegistry ssmRegistry) throws RegistryException, IOException {
    this.fs              = fs;
    this.storageLocation = storageLoc;
    Path hdfsStoragePath = new Path(storageLoc);
    if(!fs.exists(hdfsStoragePath)) {
      fs.mkdirs(hdfsStoragePath);
    }
    if(!ssmRegistry.exists()) ssmRegistry.initRegistry();
    init(ssmRegistry);
  }
  
  protected SSMWriter createWriter(String clientId, SSMRegistry registry) throws RegistryException{
    return new HdfsSSMWriter(clientId, registry, fs, storageLocation);
  }

  @Override
  protected HdfsSSMReader createReader(String clientId, SSMRegistry registry) throws RegistryException, IOException {
    return new HdfsSSMReader(clientId, registry, fs, storageLocation);
  }
  
  @Override
  protected HdfsSSMReader createReader(String clientId, SSMRegistry registry, int startFromSegmentId, long recordPos) throws RegistryException, IOException {
    return new HdfsSSMReader(clientId, registry, fs, storageLocation, startFromSegmentId, recordPos);
  }

  @Override
  protected void doDeleteSegment(String segmentId) throws IOException {
    String segmentFullPath = storageLocation + "/" + segmentId + ".dat";
    fs.delete(new Path(segmentFullPath), true);
  }
  
  @Override
  public HdfsSSMConsistencyVerifier getSegmentConsistencyVerifier() {
    return new HdfsSSMConsistencyVerifier(registry, fs, storageLocation);
  }
  
  public void dump() throws RegistryException, IOException {
    SSMRegistryPrinter rPrinter = new SSMRegistryPrinter(System.out, registry);
    rPrinter.print();
    HDFSUtil.dump(fs, storageLocation);
  }
}
