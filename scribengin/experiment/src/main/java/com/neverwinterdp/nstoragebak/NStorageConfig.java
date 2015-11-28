package com.neverwinterdp.nstoragebak;

public class NStorageConfig <T> {
  private String name;
  private String fsLocation;
  private String registryPath;
  private int    numOfPartition = 3;
  private int    maxPartitionSegmentSize = 1 * 1024 * 1024;
  private String type ;
  
  public NStorageConfig() {
  }
  
  public NStorageConfig(String name, String registryPath, String fsLocation, Class<T> type) {
    this.name = name ;
    this.registryPath = registryPath;
    this.fsLocation = fsLocation;
    this.type = type.getName();
  }

  public String getName() { return name; }
  public void   setName(String name) { this.name = name; }

  public String getFsLocation() { return fsLocation; }
  public void   setFsLocation(String fsLocation) { this.fsLocation = fsLocation; }

  public String getRegistryPath() { return registryPath; }
  public void   setRegistryPath(String registryPath) { this.registryPath = registryPath; }

  public int getNumOfPartition() { return numOfPartition; }
  public void setNumOfPartition(int numOfPartition) {
    this.numOfPartition = numOfPartition;
  }

  public int getMaxPartitionSegmentSize() { return maxPartitionSegmentSize; }
  public void setMaxPartitionSegmentSize(int maxPartitionSegmentSize) {
    this.maxPartitionSegmentSize = maxPartitionSegmentSize;
  }

  public String getType() { return type; }
  public void setType(String type) { this.type = type;}
}