package com.neverwinterdp.ssm.test;

public class TrackingRecord {
  private String writer;
  private String chunkId ;
  private int    trackingId;
  private byte[] data ;
  
  public TrackingRecord() {}
  
  public TrackingRecord(String writer, String chunkId, int trackingId, int dataSize) {
    this.writer     = writer ;
    this.chunkId    = chunkId ;
    this.trackingId = trackingId;
    this.data       = new byte[dataSize];
  }

  public String getWriter() { return writer; }
  public void setWriter(String writer) { this.writer = writer; }

  public String getChunkId() { return chunkId; }
  public void setChunkId(String chunkId) { this.chunkId = chunkId; }

  public int getTrackingId() { return trackingId; }
  public void setTrackingId(int trackingId) { this.trackingId = trackingId; }

  public byte[] getData() {  return data; }
  public void setData(byte[] data) { this.data = data; }
}
