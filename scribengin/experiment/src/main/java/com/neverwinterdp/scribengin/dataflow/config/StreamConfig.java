package com.neverwinterdp.scribengin.dataflow.config;

import java.util.Map;

import com.neverwinterdp.scribengin.storage.StorageDescriptor;

public class StreamConfig {
  private int                            parallelism ;
  private Map<String, StorageDescriptor> streams;
  
  public int getParallelism() { return parallelism; }
  public void setParallelism(int parallelism) { this.parallelism = parallelism; }
  
  public Map<String, StorageDescriptor> getStreams() { return streams; }
  public void setStreams(Map<String, StorageDescriptor> streams) { this.streams = streams; }
}
