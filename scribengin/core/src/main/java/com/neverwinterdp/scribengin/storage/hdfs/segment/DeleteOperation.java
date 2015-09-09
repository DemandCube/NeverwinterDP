package com.neverwinterdp.scribengin.storage.hdfs.segment;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DeleteOperation implements Operation {
  
  @Override
  public void execute(SegmentStorage storage, OperationConfig config) throws Exception {
    String[] source    = config.withSources();
    Path[] srcPath  = new Path[source.length];
    for(int i = 0; i < source.length; i++) {
      srcPath[i] = new Path(source[i]);
    }

    FileSystem fs = storage.getFileSystem();
    for(int i = 0; i < srcPath.length; i++) {
      fs.delete(srcPath[i], false);
    }
  }
}
