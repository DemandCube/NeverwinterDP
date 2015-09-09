package com.neverwinterdp.scribengin.storage.hdfs.segment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SegmentStorage {
  static public Comparator<Segment> COMPARATOR = new Comparator<Segment>() {
    @Override
    public int compare(Segment s1, Segment s2) {
      return 0;
    }
  };
  
  private FileSystem fs ;
  private String     location ;
  private List<Segment> segments ; 
  
  public SegmentStorage(FileSystem fs, String location) throws Exception {
    this.fs       = fs;
    this.location = location;
    refresh();
  }
  
  public FileSystem getFileSystem() { return fs; }
  
  public String getLocation() { return this.location; }
  
  public void refresh() throws Exception {
    FileStatus[] status = fs.listStatus(new Path(location));
    List<Segment> segments = new ArrayList<>();
    for (int i = 0; i < status.length; i++) {
      String segmentName = status[i].getPath().getName();
      if(segmentName.startsWith("segment-") && segmentName.endsWith(".data")) {
        segments.add(new Segment(status[i]));
      }
    }
    this.segments = segments;
  }
  
  
  public HDFSSegmentSet getBufferSegments() {
    return getSegmentByType(Segment.Type.buffer);
  }
  
  public HDFSSegmentSet getSmallSegments() {
    return getSegmentByType(Segment.Type.small);
  }
  
  public HDFSSegmentSet getMediumSegments() {
    return getSegmentByType(Segment.Type.medium);
  }
  
  public HDFSSegmentSet getLargeSegments() {
    return getSegmentByType(Segment.Type.large);
  }
  
  public HDFSSegmentSet getSegmentByType(Segment.Type type) {
    List<Segment> holder = new ArrayList<>();
    for(int i = 0; i < segments.size(); i++) {
      Segment sel = segments.get(i);
      if(sel.getType() == type) holder.add(sel);
    }
    return new HDFSSegmentSet(holder);
  }
  
  public void optimizeBufferSegments() throws Exception {
    SegmentStorage.HDFSSegmentSet bufferSegments = getBufferSegments();
    if(bufferSegments.getDataSize() >= Segment.SMALL_DATASIZE_THRESHOLD) {
      List<Segment> segments = bufferSegments.getSegments();
      Segment destSegment = new Segment();
      destSegment.setType(Segment.Type.small);
      merge(segments, destSegment);
    }
    refresh();
  }
  
  void merge(List<Segment> srcSegments, Segment destSegment) throws Exception {
    List<String> dataPathHolder = new ArrayList<>();
    for (int i = 0; i < srcSegments.size(); i++) {
      Segment segment = srcSegments.get(i);  
      dataPathHolder.add(segment.toDataPath(location));
    }
    
    BatchOperation batch = new BatchOperation();
    batch.add(
        new OperationConfig("buffering", MergeOperation.class).
        withSources(dataPathHolder).
        withDestination(destSegment.toBufferingPath(location))
    );
    batch.add(
        new OperationConfig("buffer-complete", RenameOperation.class).
        withSource(destSegment.toBufferingPath(location)).
        withDestination(destSegment.toCompletePath(location)));
    batch.add(
        new OperationConfig("delete-buffer", DeleteOperation.class).
        withSources(dataPathHolder)
        );
    batch.add(
        new OperationConfig("commit-data", RenameOperation.class).
        withSource(destSegment.toCompletePath(location)).
        withDestination(destSegment.toDataPath(location))
        );
    batch.execute(this);
  }
  
  public WriteLock getWriteLock(long maxWaitime) throws IllegalArgumentException, IOException {
    Path lockWritePath = new Path(location + "/lock.write");
    return new WriteLock(lockWritePath) ;
  }
  
  public class WriteLock {
    private Path writeLock ;
    private boolean owner = false;
    
    public WriteLock(Path writeLock) {
      this.writeLock = writeLock;
    }

    public boolean tryLock(long timeout, long tryPeriod) throws IOException, InterruptedException {
      long stopTime = System.currentTimeMillis() + timeout;
      while(stopTime > System.currentTimeMillis()) {
        boolean locked = lock() ;
        if(locked) return true;
        Thread.sleep(tryPeriod);
      }
      return false; 
    }
    
    synchronized public boolean lock() throws IOException {
      if(owner) return true;
      owner = fs.createNewFile(writeLock);
      return owner;
    }
    
    synchronized public boolean unlock() throws IOException {
      boolean deleted = fs.delete(writeLock, false);
      owner = !deleted;
      return deleted;
    }
    
    synchronized void discardDeathLock() throws IOException {
      if(fs.exists(writeLock)) {
      }
    }
    
  }
  
  static public class HDFSSegmentSet {
    private List<Segment> segments;
    
    public HDFSSegmentSet(List<Segment> segments) {
      this.segments = segments;
    }
    
    public List<Segment> getSegments() { return this.segments; }

    public long getDataSize() {
      long dataSize = 0;
      for(int i = 0; i < segments.size(); i++) {
        Segment sel = segments.get(i);
        dataSize += sel.getDataSize();
      }
      return dataSize;
    }
  }
}