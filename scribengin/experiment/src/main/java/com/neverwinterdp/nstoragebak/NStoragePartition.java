package com.neverwinterdp.nstoragebak;

import java.util.Comparator;

public class NStoragePartition {
  static public Comparator<NStoragePartition> PARTITION_ID_COMPARATOR = new Comparator<NStoragePartition>() {
    @Override
    public int compare(NStoragePartition p1, NStoragePartition p2) {
      return p1.partitionId - p2.partitionId;
    }
  };
  
  private int    partitionId;
  private String registryLocation;
  private String fsLocation;

  public NStoragePartition() {
  }
  
  public NStoragePartition(int partitionId, String registryLocation, String fsLocation) {
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
