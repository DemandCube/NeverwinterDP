package com.neverwinterdp.scribengin.storage.hdfs.segment;

import org.apache.hadoop.fs.Path;

import com.neverwinterdp.vm.environment.yarn.HDFSUtil;

public class MergeOperation implements Operation {
  
  @Override
  public void execute(SegmentStorage storage, OperationConfig config) throws Exception {
    String   destination = config.getDestination();
    String[] sources     = config.withSources();
    
    Path   destPath = new Path(destination);
    Path[] srcPath  = new Path[sources.length];
    for(int i = 0; i < sources.length; i++) {
      srcPath[i] = new Path(sources[i]);
    }
    HDFSUtil.concat(storage.getFileSystem(), destPath, srcPath);
  }
}
