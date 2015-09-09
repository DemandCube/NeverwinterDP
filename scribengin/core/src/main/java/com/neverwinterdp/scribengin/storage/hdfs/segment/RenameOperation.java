package com.neverwinterdp.scribengin.storage.hdfs.segment;

import org.apache.hadoop.fs.Path;

public class RenameOperation implements Operation {
  
  @Override
  public void execute(SegmentStorage storage, OperationConfig config) throws Exception {
    Path source      = new Path(config.withSource()) ;
    Path destination = new Path(config.getDestination()) ;
    storage.getFileSystem().rename(source, destination);
  }
}
