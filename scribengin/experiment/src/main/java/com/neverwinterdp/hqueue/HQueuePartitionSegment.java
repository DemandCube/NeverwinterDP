package com.neverwinterdp.hqueue;

import java.util.Comparator;

public class HQueuePartitionSegment {
  static public Comparator<HQueuePartitionSegment> ID_COMPARATOR = new Comparator<HQueuePartitionSegment>() {
    @Override
    public int compare(HQueuePartitionSegment s1, HQueuePartitionSegment s2) {
      return s1.getId() - s2.getId();
    }
  };
  
  private int    id       ;
  private String registryPath;
  private String fsPath ;
  private long   available;

  public int getId() { return id; }
  public void setId(int id) { this.id = id; }

  public String getRegistryPath() { return registryPath; }
  public void setRegistryPath(String registryPath) { this.registryPath = registryPath; }

  public String getFsPath() { return fsPath; }
  public void setFsPath(String fsPath) { this.fsPath = fsPath; }

  public long getAvailable() { return available; }
  public void setAvailable(long available) {
    this.available = available;
  }
}
