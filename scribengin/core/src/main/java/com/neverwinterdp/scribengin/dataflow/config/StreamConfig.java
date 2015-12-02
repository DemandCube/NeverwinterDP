package com.neverwinterdp.scribengin.dataflow.config;

import java.util.Map;

import com.neverwinterdp.storage.StorageConfig;

public class StreamConfig {
  private int                        parallelism;
  private int                        replication = 2;
  private Map<String, StorageConfig> streams;
  
  public int getParallelism() { return parallelism; }
  public void setParallelism(int parallelism) { this.parallelism = parallelism; }
  
  public int getReplication() { return replication; }
  public void setReplication(int replication) { this.replication = replication; }
  
  public Map<String, StorageConfig> getStreams() { return streams; }
  public void setStreams(Map<String, StorageConfig> streams) { this.streams = streams; }
}
