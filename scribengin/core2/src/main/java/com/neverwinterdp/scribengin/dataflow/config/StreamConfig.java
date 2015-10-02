package com.neverwinterdp.scribengin.dataflow.config;

import java.util.Map;

import com.neverwinterdp.scribengin.storage.StorageConfig;

public class StreamConfig {
  private int                            parallelism ;
  private Map<String, StorageConfig> streams;
  
  public int getParallelism() { return parallelism; }
  public void setParallelism(int parallelism) { this.parallelism = parallelism; }
  
  public Map<String, StorageConfig> getStreams() { return streams; }
  public void setStreams(Map<String, StorageConfig> streams) { this.streams = streams; }
}
