package com.neverwinterdp.storage.simplehdfs;

import java.util.List;

import org.apache.hadoop.fs.Path;

public class SegmentSet {
  private List<Segment> segments;
  
  public SegmentSet() {
  }
  
  public SegmentSet(List<Segment> segments) {
    this.segments = segments;
  }
  
  public List<Segment> getSegments() { return this.segments; }
  public void setSegments(List<Segment> segments) {
    this.segments = segments;
  }

  public long dataSize() {
    long dataSize = 0;
    for(int i = 0; i < segments.size(); i++) {
      Segment sel = segments.get(i);
      dataSize += sel.getDataSize();
    }
    return dataSize;
  }
  
  public Path[] toHDFSDataPath(SegmentStorage<?> storage) {
    Path[] path = new Path[segments.size()];
    String location = storage.getLocation();
    for(int i = 0; i < segments.size(); i++) {
      path[i] = new Path(segments.get(i).toDataPath(location));
    }
    return path;
  }
}