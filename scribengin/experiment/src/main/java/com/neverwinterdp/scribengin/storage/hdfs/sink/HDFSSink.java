package com.neverwinterdp.scribengin.storage.hdfs.sink;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedHashMap;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.PartitionDescriptor;
import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStream;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.vm.environment.yarn.HDFSUtil;

public class HDFSSink implements Sink {
  private FileSystem fs;
  private StorageDescriptor descriptor;

  private int idTracker = 0;
  private LinkedHashMap<Integer, HDFSSinkPartitionStream> streams = new LinkedHashMap<Integer, HDFSSinkPartitionStream>() ;
  
  public HDFSSink(FileSystem fs, String location) throws FileNotFoundException, IllegalArgumentException, IOException {
    this(fs, new StorageDescriptor("HDFS", location));
  }
  
  public HDFSSink(FileSystem fs, PartitionDescriptor streamDescriptor) throws FileNotFoundException, IllegalArgumentException, IOException {
    this(fs, getSinkDescriptor(streamDescriptor));
  }
  
  public HDFSSink(FileSystem fs, StorageDescriptor descriptor) throws FileNotFoundException, IllegalArgumentException, IOException {
    this.fs = fs;
    this.descriptor = descriptor;
   
    Path fsLoc = new Path(descriptor.getLocation());
    if(!fs.exists(fsLoc)) fs.mkdirs(fsLoc) ;
    FileStatus[] status = fs.listStatus(fsLoc) ;
    for(int i = 0; i < status.length; i++) {
      HDFSSinkPartitionStream stream = new HDFSSinkPartitionStream(fs, status[i].getPath());
      streams.put(stream.getDescriptor().getId(), stream);
    }
  }
  
  public StorageDescriptor getDescriptor() { return this.descriptor; }
  
  public SinkPartitionStream  getStream(PartitionDescriptor descriptor) throws Exception {
    SinkPartitionStream stream = streams.get(descriptor.getId());
    if(stream == null) {
      throw new Exception("Cannot find the stream " + descriptor.getId()) ;
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
    SinkPartitionStream foundStream = streams.remove(stream.getDescriptor().getId()) ;
    if(foundStream == null) {
      throw new Exception("Cannot find the stream " + stream.getDescriptor().getId()) ;
    }
  }
  
  @Override
  synchronized public SinkPartitionStream newStream() throws IOException {
    int id = idTracker++;
    String location = descriptor.getLocation() + "/stream-" + id;
    PartitionDescriptor streamDescriptor = new PartitionDescriptor("HDFS", id, location) ;
    HDFSSinkPartitionStream stream = new HDFSSinkPartitionStream(fs, streamDescriptor);
    streams.put(streamDescriptor.getId(), stream) ;
    return stream;
  }

  @Override
  public void close() throws Exception  { 
  }
  
  public void fsCheck() throws Exception {
  }
  
  static StorageDescriptor getSinkDescriptor(PartitionDescriptor streamDescriptor) {
    String location = streamDescriptor.getLocation();
    location = location.substring(0, location.lastIndexOf('/'));
    StorageDescriptor descriptor = new StorageDescriptor(streamDescriptor.getType(), location);
    return descriptor;
  }
}