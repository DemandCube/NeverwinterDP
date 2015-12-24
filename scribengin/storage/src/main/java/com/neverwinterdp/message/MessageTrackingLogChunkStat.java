package com.neverwinterdp.message;

public class MessageTrackingLogChunkStat {
  private int  count = 0;
  private long avgDeliveryTime = 0l;
  private long sumAvgDeliveryTime = 0l;
  
  public MessageTrackingLogChunkStat() {
  }

  public int getCount() { return count; }
  public void setCount(int count) { this.count = count; }

  public long getAvgDeliveryTime() { return avgDeliveryTime; }
  public void setAvgDeliveryTime(long avgDeliveryTime) { this.avgDeliveryTime = avgDeliveryTime; }

  public void log(MessageTracking mTracking, MessageTrackingLog log) {
    count++ ;
    sumAvgDeliveryTime += log.getTimestamp() - mTracking.getTimestamp() ;
    avgDeliveryTime = sumAvgDeliveryTime/count;
  }
  
  public void merge(MessageTrackingLogChunkStat other) {
    int  newCount = count + other.count;
    avgDeliveryTime = ((count * avgDeliveryTime) + (other.count * other.avgDeliveryTime))/newCount;
    count           = newCount;
  }
}
