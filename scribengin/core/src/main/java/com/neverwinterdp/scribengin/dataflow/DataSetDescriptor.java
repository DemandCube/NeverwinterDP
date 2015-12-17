package com.neverwinterdp.scribengin.dataflow;

import java.util.HashMap;
import java.util.Map;

import com.neverwinterdp.storage.StorageConfig;

public class DataSetDescriptor {
  private int                        parallelism;
  private int                        replication = 2;
  private Map<String, StorageConfig> streams;
  
  public int getParallelism() { return parallelism; }
  public void setParallelism(int parallelism) { this.parallelism = parallelism; }
  
  public int getReplication() { return replication; }
  public void setReplication(int replication) { this.replication = replication; }
  
  public Map<String, StorageConfig> getStreams() { return streams; }
  public void setStreams(Map<String, StorageConfig> streams) { this.streams = streams; }
  
  public void add(String name, StorageConfig sconfig) {
    if(streams == null) streams = new HashMap<>();
    streams.put(name, sconfig);
  }
  
  public void clear() { 
    if(streams != null) streams.clear();
  }
}
