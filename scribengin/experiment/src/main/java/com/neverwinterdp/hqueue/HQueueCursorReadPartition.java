package com.neverwinterdp.hqueue;

public class HQueueCursorReadPartition {
  private String name ;
  private int    partitionId ;
  private int    currentSegment;
  private int    currentPosition;
  
  public HQueueCursorReadPartition() {
  }
  
  public HQueueCursorReadPartition(String name) {
    this.name = name;
  }

  public String getName() { return name; }
  public void setName(String name) { this.name = name; }

  public int getPartitionId() { return partitionId; }
  public void setPartitionId(int partitionId) { this.partitionId = partitionId; }

  public int getCurrentSegment() { return currentSegment; }
  public void setCurrentSegment(int currentSegment) { this.currentSegment = currentSegment; }

  public int getCurrentPosition() { return currentPosition; }
  public void setCurrentPosition(int currentPosition) { this.currentPosition = currentPosition; }
}