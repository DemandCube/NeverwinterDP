package com.neverwinterdp.scribengin.dataflow.tool.tracking;

public class TrackingMessage {
  private String vmId ;
  private String chunkId ;
  private int    trackId ;
  
  private long   startDeliveryTime;
  private long   endDeliveryTime;
  
  private byte[] data ;
  
  public TrackingMessage() { }
  
  public TrackingMessage(String vmId, String chunkId, int trackId, byte[] data) {
    this.vmId = vmId;
    this.chunkId = chunkId;
    this.trackId = trackId;
    this.data = data;
  }

  public String getVmId() { return vmId; }
  public void setVmId(String vmId) { this.vmId = vmId; }

  public String getChunkId() { return chunkId;}
  public void setChunkId(String chunkId) { this.chunkId = chunkId; }

  public long getStartDeliveryTime() { return startDeliveryTime; }
  public void setStartDeliveryTime(long time) { this.startDeliveryTime = time; }

  public long getEndDeliveryTime() { return endDeliveryTime; }
  public void setEndDeliveryTime(long time) { this.endDeliveryTime = time; }

  public int getTrackId() { return trackId; }
  public void setTrackId(int trackId) { this.trackId = trackId;}

  public byte[] getData() { return data; }
  public void setData(byte[] data) { this.data = data; }
  
  public String messageKey() { return vmId + ":" + chunkId + ":" + trackId; }
  
  public String reportName() {  return vmId + "." + chunkId ; }
  
  public long deliveryTime() { return endDeliveryTime - startDeliveryTime; }
}
