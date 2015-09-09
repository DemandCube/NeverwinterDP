package com.neverwinterdp.scribengin.storage.hdfs.segment;

public interface Operation {
  public void execute(SegmentStorage storage, OperationConfig config) throws Exception ;
}
