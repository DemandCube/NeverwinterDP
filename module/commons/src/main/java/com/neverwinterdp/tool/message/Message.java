package com.neverwinterdp.tool.message;

public class Message {
  private String partition ;
  private int    trackId ;
  private byte[] data ;
  
  public Message() { }
  
  public Message(int partition, int trackId, int messageSize) {
    this(Integer.toString(partition), trackId, messageSize);
  }
  
  public Message(String partition, int trackId, int messageSize) {
    this.partition = partition ;
    this.trackId = trackId ;
    this.data    = new byte[messageSize] ;
  }

  public String getPartition() { return partition; }
  public void setPartition(String partition) { this.partition = partition; }

  public int  getTrackId() { return trackId; }
  public void setTrackId(int trackId) { this.trackId = trackId; }

  public byte[] getData() { return data; }
  public void setData(byte[] data) { this.data = data; }
}