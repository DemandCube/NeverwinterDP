package com.neverwinterdp.storage.simplehdfs.source;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.source.SourcePartition;
import com.neverwinterdp.vm.environment.yarn.HDFSUtil;

/**
 * @author Tuan Nguyen
 */
public class HDFSSourcePartition implements SourcePartition {
  private FileSystem    fs;
  private String        partitionLocation ;
  private StorageConfig storageConfig ;
  
  public HDFSSourcePartition(FileSystem fs, StorageConfig descriptor, String partitionLoc) throws Exception {
    this.fs = fs;
    this.storageConfig = descriptor ;
    this.partitionLocation = partitionLoc;
  }
  
  public String getPartitionLocation() { return partitionLocation; }
  
  public StorageConfig getStorageConfig() { return storageConfig; }

  public HDFSSourcePartitionStream  getPartitionStream(int id) { 
    PartitionStreamConfig pConfig = new PartitionStreamConfig();
    pConfig.setLocation(partitionLocation + "/partition-stream-" + id);
    pConfig.setPartitionStreamId(id);
    HDFSSourcePartitionStream stream = new HDFSSourcePartitionStream(fs, pConfig);
    return stream; 
  }
  
  public HDFSSourcePartitionStream   getPartitionStream(PartitionStreamConfig descriptor) { 
    return getPartitionStream(descriptor.getPartitionStreamId()) ; 
  }
  
  public HDFSSourcePartitionStream[] getPartitionStreams() throws Exception {
    Path fsLoc = new Path(partitionLocation);
    if(!fs.exists(fsLoc)) {
      throw new Exception("location " + partitionLocation + " does not exist!") ;
    }
    
    FileStatus[] status = fs.listStatus(new Path(partitionLocation)) ;
    HDFSSourcePartitionStream[] stream = new HDFSSourcePartitionStream[status.length];
    for(int i = 0; i < status.length; i++) {
      PartitionStreamConfig pConfig = new PartitionStreamConfig();
      pConfig.setLocation(status[i].getPath().toString());
      pConfig.setPartitionStreamId(HDFSUtil.getStreamId(status[i].getPath()));
      stream[i] = new HDFSSourcePartitionStream(fs, pConfig);
    }
    return stream;
  }
  
  public void delete() throws Exception {
    fs.delete(new Path(partitionLocation), true);
  }
  
  public void close() throws Exception {
  }
}
