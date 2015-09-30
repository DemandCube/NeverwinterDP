package com.neverwinterdp.scribengin.storage.hdfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Storage<T> {
  private FileSystem    fs;
  private String        location;
  private Class<T>      type;
  private LinkedHashMap<String, Segment> segments;
  
  public Storage(FileSystem fs, String location, Class<T> type) throws IOException {
    this.fs       = fs;
    this.location = location;
    this.type = type;
    refresh();
  }
  
  public FileSystem getFileSystem() { return fs; }
  
  public String getLocation() { return this.location; }
  
  synchronized public void refresh() throws IOException {
    Path locPath = new Path(location) ;
    if(!fs.exists(locPath)) {
      fs.mkdirs(locPath);
      this.segments = new LinkedHashMap<>();
    } else {
      FileStatus[] status = fs.listStatus(locPath);
      List<Segment> segments = new ArrayList<>();
      for (int i = 0; i < status.length; i++) {
        String segmentName = status[i].getPath().getName();
        if(segmentName.startsWith("segment-") && segmentName.endsWith(".data")) {
          segments.add(new Segment(status[i]));
        }
      }
      Collections.sort(segments, Segment.COMPARATOR);
      LinkedHashMap<String, Segment> segmentMap = new LinkedHashMap<>();
      for(Segment sel : segments) segmentMap.put(sel.getUuid(), sel);
      this.segments = segmentMap;
    }
  }
  
  public StorageWriter<T> getStorageWriter() { return new StorageWriter<T>(this); }
  
  public SegmentSet getBufferSegments() {
    return getSegmentByType(Segment.Type.buffer);
  }
  
  public SegmentSet getSmallSegments() {
    return getSegmentByType(Segment.Type.small);
  }
  
  public SegmentSet getMediumSegments() {
    return getSegmentByType(Segment.Type.medium);
  }
  
  public SegmentSet getLargeSegments() {
    return getSegmentByType(Segment.Type.large);
  }
  
  public SegmentSet getSegmentByType(Segment.Type type) {
    List<Segment> holder = new ArrayList<>();
    for(Segment sel : segments.values()) {
      if(sel.getType() == type) holder.add(sel);
    }
    return new SegmentSet(holder);
  }
  
  public void optimizeBufferSegments() throws Exception {
    OperationConfig opConfig = MergeOperation.createOperationConfig(this, Segment.Type.buffer, 30000);
    execute(opConfig, 30000, 1000);
  }
  
  public void optimizeSmallSegments() throws Exception {
    OperationConfig opConfig = MergeOperation.createOperationConfig(this, Segment.Type.small, 30000);
    execute(opConfig, 30000, 1000);
  }
  
  public void optimizeMediumSegments() throws Exception {
    OperationConfig opConfig = MergeOperation.createOperationConfig(this, Segment.Type.medium, 30000);
    execute(opConfig, 30000, 1000);
  }
  
  
  public void execute(OperationConfig config, long maxWaitTime, long tryPeriod) throws Exception {
    Path lockPath = new Path(location + "/lock");
    Lock lock = new Lock(fs, lockPath, config) ;
    if(lock.tryLock(maxWaitTime, tryPeriod)) {
      Class<? extends OperationExecutor> opClass = 
        (Class<? extends OperationExecutor>) Class.forName(config.getExecutor()) ;
      OperationExecutor op = opClass.newInstance();
      op.execute(this, lock, config);
      lock.unlock();
    }
  }
}