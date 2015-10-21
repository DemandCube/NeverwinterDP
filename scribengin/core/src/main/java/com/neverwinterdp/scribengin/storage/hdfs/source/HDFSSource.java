package com.neverwinterdp.scribengin.storage.hdfs.source;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.scribengin.storage.PartitionConfig;
import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.source.Source;
import com.neverwinterdp.scribengin.storage.source.SourcePartitionStream;
import com.neverwinterdp.vm.environment.yarn.HDFSUtil;

/**
 * @author Tuan Nguyen
 */
public class HDFSSource implements Source {
  private FileSystem fs;
  private StorageConfig descriptor ;
  private Map<Integer,HDFSSourcePartitionStream> streams = new LinkedHashMap<Integer, HDFSSourcePartitionStream>();
  
  public HDFSSource(FileSystem fs, String location) throws Exception {
    this(fs, new StorageConfig("HDFS", location));
  }
  
  public HDFSSource(FileSystem fs, StorageConfig descriptor) throws Exception {
    this.fs = fs;
    this.descriptor = descriptor ;
    Path fsLoc = new Path(descriptor.getLocation());
    if(!fs.exists(fsLoc)) {
      throw new Exception("location " + descriptor.getLocation() + " does not exist!") ;
    }
    
    FileStatus[] status = fs.listStatus(new Path(descriptor.getLocation())) ;
    for(int i = 0; i < status.length; i++) {
      PartitionConfig pConfig = new PartitionConfig();
      pConfig.setLocation(status[i].getPath().toString());
      pConfig.setPartitionId(HDFSUtil.getStreamId(status[i].getPath()));
      HDFSSourcePartitionStream stream = new HDFSSourcePartitionStream(fs, pConfig);
      streams.put(pConfig.getPartitionId(), stream);
    }
  }
  
  public StorageConfig getStorageConfig() { return descriptor; }

  public SourcePartitionStream   getStream(int id) { return streams.get(id) ; }
  
  public SourcePartitionStream   getStream(PartitionConfig descriptor) { 
    return streams.get(descriptor.getPartitionId()) ; 
  }
  
  public SourcePartitionStream[] getStreams() {
    SourcePartitionStream[] array = new SourcePartitionStream[streams.size()];
    return streams.values().toArray(array);
  }
  
  public void close() throws Exception {
  }
}
