package com.neverwinterdp.hqueue;

import java.util.Comparator;

public class HQueuePartition {
  static public Comparator<HQueuePartition> PARTITION_ID_COMPARATOR = new Comparator<HQueuePartition>() {
    @Override
    public int compare(HQueuePartition p1, HQueuePartition p2) {
      return p1.partitionId - p2.partitionId;
    }
  };
  
  private int    partitionId;
  private String registryLocation;
  private String fsLocation;

  public HQueuePartition() {
  }
  
  public HQueuePartition(int partitionId, String registryLocation, String fsLocation) {
    this.partitionId = partitionId ;
    this.registryLocation = registryLocation;
    this.fsLocation = fsLocation;
  }

  public int getPartitionId() { return partitionId; }
  public void setPartitionId(int partitionId) { this.partitionId = partitionId; }

  public String getRegistryLocation() { return registryLocation; }
  public void setRegistryLocation(String registryLocation) { 
    this.registryLocation = registryLocation;
  }

  public String getFsLocation() { return fsLocation; }
  public void setFsLocation(String fsLocation) {
    this.fsLocation = fsLocation;
  }
}
