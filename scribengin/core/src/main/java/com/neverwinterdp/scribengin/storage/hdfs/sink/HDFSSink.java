package com.neverwinterdp.scribengin.storage.hdfs.sink;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedHashMap;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.scribengin.storage.PartitionConfig;
import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStream;

public class HDFSSink implements Sink {
  private FileSystem fs;
  private StorageConfig storageConfig;

  private int idTracker = 0;
  private LinkedHashMap<Integer, HDFSSinkPartitionStream> streams = new LinkedHashMap<Integer, HDFSSinkPartitionStream>() ;
  
  public HDFSSink(FileSystem fs, String location) throws FileNotFoundException, IllegalArgumentException, IOException {
    this(fs, new StorageConfig("HDFS", location));
  }
  
  public HDFSSink(FileSystem fs, StorageConfig sConfig) throws FileNotFoundException, IllegalArgumentException, IOException {
    this.fs = fs;
    this.storageConfig = sConfig;
   
    Path fsLoc = new Path(sConfig.getLocation());
    if(!fs.exists(fsLoc)) fs.mkdirs(fsLoc) ;
    FileStatus[] status = fs.listStatus(fsLoc) ;
    for(int i = 0; i < status.length; i++) {
      HDFSSinkPartitionStream stream = new HDFSSinkPartitionStream(fs, status[i].getPath());
      streams.put(stream.getParitionConfig().getPartitionId(), stream);
    }
  }
  
  public StorageConfig getDescriptor() { return this.storageConfig; }
  
  public SinkPartitionStream  getStream(PartitionConfig descriptor) throws Exception {
    return getStream(descriptor.getPartitionId());
  }
  
  public SinkPartitionStream  getStream(int partitionId) throws Exception {
    SinkPartitionStream stream = streams.get(partitionId);
    if(stream == null) {
      throw new Exception("Cannot find the stream " + partitionId) ;
    }
    return stream ;
  }
  
  synchronized public SinkPartitionStream[] getStreams() {
    SinkPartitionStream[] array = new SinkPartitionStream[streams.size()] ;
    streams.values().toArray(array) ;
    return array;
  }

  @Override
  synchronized public void delete(SinkPartitionStream stream) throws Exception {
    SinkPartitionStream foundStream = streams.remove(stream.getParitionConfig().getPartitionId()) ;
    if(foundStream == null) {
      throw new Exception("Cannot find the stream " + stream.getParitionConfig().getPartitionId()) ;
    }
  }
  
  @Override
  synchronized public SinkPartitionStream newStream() throws IOException {
    int id = idTracker++;
    String location = storageConfig.getLocation() + "/stream-" + id;
    PartitionConfig pConfig = new PartitionConfig(id, location) ;
    HDFSSinkPartitionStream stream = new HDFSSinkPartitionStream(fs, pConfig);
    streams.put(pConfig.getPartitionId(), stream) ;
    return stream;
  }

  @Override
  public void close() throws Exception  { 
  }
  
  public void fsCheck() throws Exception {
  }
}