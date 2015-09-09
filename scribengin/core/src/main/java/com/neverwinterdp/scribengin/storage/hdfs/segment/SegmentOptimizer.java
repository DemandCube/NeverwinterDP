package com.neverwinterdp.scribengin.storage.hdfs.segment;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.vm.environment.yarn.HDFSUtil;

public class SegmentOptimizer {
  private FileSystem       fs;
  private StreamDescriptor descriptor;
  
  public SegmentOptimizer(FileSystem fs, StreamDescriptor descriptor) {
    this.fs         = fs ;
    this.descriptor = descriptor;
  }

  public void optimize() throws Exception {
    SegmentStorage segmentStorage = new SegmentStorage(fs, descriptor.getLocation());
    optimizeBuffer(segmentStorage);
  }
  
  void optimizeBuffer(SegmentStorage segmentStorage) throws Exception {
    SegmentStorage.HDFSSegmentSet bufferSegments = segmentStorage.getBufferSegments();
    if(bufferSegments.getDataSize() >= Segment.SMALL_DATASIZE_THRESHOLD) {
      segmentStorage.optimizeBufferSegments();
    }
  }
  
  public void lock() throws Exception {
    fs.createNewFile(new Path(""));
  }
  
  public void unlock() throws Exception {
  }
  
  public class Lock {
  }
}