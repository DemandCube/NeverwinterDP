package com.neverwinterdp.message;

public class MessageTrackingLogChunkStat {
  private int  count;
  private long avgDeliveryTime;
  private long sumAvgDeliveryTime;
  
  public MessageTrackingLogChunkStat() {
  }

  public int getCount() { return count; }
  public void setCount(int count) { this.count = count; }

  public long getAvgDeliveryTime() { return avgDeliveryTime; }
  public void setAvgDeliveryTime(long avgDeliveryTime) { this.avgDeliveryTime = avgDeliveryTime; }

  public void log(MessageTracking mTracking, MessageTrackingLog log) {
    count++ ;
    sumAvgDeliveryTime += mTracking.getTimestamp() - log.getTimestamp();
    avgDeliveryTime = sumAvgDeliveryTime/count;
  }
  
  public void merge(MessageTrackingLogChunkStat other) {
    int  newCount = count + other.count;
    long newAvgDeliveryTime = ((count * avgDeliveryTime) + (other.count * other.avgDeliveryTime))/newCount;
    this.count           = newCount;
    this.avgDeliveryTime = newAvgDeliveryTime;
  }
}
