package com.neverwinterdp.storage.simplehdfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.vm.environment.yarn.HDFSUtil;

public class SegmentMergeOperation<T> implements SegmentOperationExecutor<T> {
  
  @Override
  public void execute(SegmentStorage<T> storage, SegmentLock lock, SegmentOperationConfig config) throws Exception {
    if(!init(storage,lock, config)) return;
    
    config.withAttribute("step", "merge");
    lock.update(config);
    merge(storage,lock, config);
    
    config.withAttribute("step", "commit");
    lock.update(config);
    commit(storage,lock, config);
  }

  public void resume(SegmentStorage<T> storage, SegmentLock lock, SegmentOperationConfig config) throws Exception {
  }
  
  boolean init(SegmentStorage<T> storage, SegmentLock lock, SegmentOperationConfig config) throws Exception {
    storage.refresh();
    Segment.Type segType = config.attribute("segment.type", Segment.Type.class);

    Segment destSegment = new Segment() ;
    destSegment.setType(segType.nextLargerType());
    
    SegmentSet segSet = storage.getSegmentByType(segType);
    List<Segment> segments = segSet.getSegments() ;
    List<Segment> selectSegments = new ArrayList<>();
    long destSegmentLimit = Segment.getSegmentDataSizeThreshold(destSegment.getType());
    long accumulate = 0 ;
    for(int i = 0; i < segments.size(); i++) {
      Segment segment = segments.get(i);
      accumulate += segment.getDataSize();
      selectSegments.add(segment);
      if(accumulate >= destSegmentLimit) {
        break;
      }
    }
    if(accumulate < destSegmentLimit) return false; 
    
    config.withAttribute("sources",     new SegmentSet(selectSegments));
    config.withAttribute("destination", destSegment);
    return true;
  }
  
  void merge(SegmentStorage<T> storage, SegmentLock lock, SegmentOperationConfig config) throws Exception {
    SegmentSet segSet = config.attribute("sources", SegmentSet.class);
    Segment    destSegment = config.attribute("destination", Segment.class);
    
    Path   bufferingPath = new Path(destSegment.toBufferingPath(storage.getLocation()));
    Path   completePath = new Path(destSegment.toCompletePath(storage.getLocation()));
    Path[] srcPath  = segSet.toHDFSDataPath(storage);
    
    FileSystem fs = storage.getFileSystem();
    HDFSUtil.concat(fs, bufferingPath, srcPath);
    fs.rename(bufferingPath, completePath);
  }
  
  void commit(SegmentStorage<T> storage, SegmentLock lock, SegmentOperationConfig config) throws Exception {
    FileSystem fs = storage.getFileSystem();
//    SegmentSet segSet = config.attribute("sources", SegmentSet.class);
//    Path[] srcPath  = segSet.toHDFSDataPath(storage);
//    for(int i = 0; i < srcPath.length; i++) {
//      boolean deleted = fs.delete(srcPath[i], false);
//      if(!deleted) {
//        throw new IOException("Cannot delete " + srcPath[i]) ;
//      }
//    }
    
    Segment destSegment = config.attribute("destination", Segment.class);
    Path completePath = new Path(destSegment.toCompletePath(storage.getLocation()));
    Path dataPath = new Path(destSegment.toDataPath(storage.getLocation()));
    fs.rename(completePath, dataPath);
  }
  
  static public SegmentOperationConfig createOperationConfig(SegmentStorage<?> storage, Segment.Type segType, long maxLockTime) {
    SegmentOperationConfig opConfig = new SegmentOperationConfig("merge-buffer", maxLockTime);
    opConfig.
      withExecutor(SegmentMergeOperation.class).
      withAttribute("segment.type", segType);
    return opConfig;
  }
}
