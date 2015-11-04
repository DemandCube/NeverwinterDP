package com.neverwinterdp.scribengin.storage.hdfs.source;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.scribengin.storage.PartitionStreamConfig;
import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.source.SourcePartition;
import com.neverwinterdp.scribengin.storage.source.SourcePartitionStream;
import com.neverwinterdp.vm.environment.yarn.HDFSUtil;

/**
 * @author Tuan Nguyen
 */
public class HDFSSourcePartition implements SourcePartition {
  private FileSystem fs;
  private String     location ;
  private StorageConfig descriptor ;
  private Map<Integer,HDFSSourcePartitionStream> streams = new LinkedHashMap<Integer, HDFSSourcePartitionStream>();
  
  public HDFSSourcePartition(FileSystem fs, String location) throws Exception {
    this(fs, new StorageConfig("HDFS", location));
  }
  
  public HDFSSourcePartition(FileSystem fs, StorageConfig descriptor) throws Exception {
    this.fs = fs;
    this.descriptor = descriptor ;
    Path fsLoc = new Path(descriptor.getLocation());
    if(!fs.exists(fsLoc)) {
      throw new Exception("location " + descriptor.getLocation() + " does not exist!") ;
    }
    
    FileStatus[] status = fs.listStatus(new Path(descriptor.getLocation())) ;
    for(int i = 0; i < status.length; i++) {
      PartitionStreamConfig pConfig = new PartitionStreamConfig();
      pConfig.setLocation(status[i].getPath().toString());
      pConfig.setPartitionStreamId(HDFSUtil.getStreamId(status[i].getPath()));
      HDFSSourcePartitionStream stream = new HDFSSourcePartitionStream(fs, pConfig);
      streams.put(pConfig.getPartitionStreamId(), stream);
    }
  }
  
  public StorageConfig getStorageConfig() { return descriptor; }

  public SourcePartitionStream   getPartitionStream(int id) { 
    return streams.get(id) ; 
  }
  
  public SourcePartitionStream   getPartitionStream(PartitionStreamConfig descriptor) { 
    return streams.get(descriptor.getPartitionStreamId()) ; 
  }
  
  public SourcePartitionStream[] getPartitionStreams() throws Exception {
    SourcePartitionStream[] array = new SourcePartitionStream[streams.size()];
    return streams.values().toArray(array);
  }
  
  public void close() throws Exception {
  }
}
