package com.neverwinterdp.message;

import com.neverwinterdp.message.MessageTracking;
import com.neverwinterdp.message.MessageTrackingLog;

public class TrackingWindowLogStat {
  private int  count = 0;
  private long avgDeliveryTime = 0l;
  private long sumAvgDeliveryTime = 0l;
  
  public TrackingWindowLogStat() {
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
  
  public void merge(TrackingWindowLogStat other) {
    int  newCount = count + other.count;
    avgDeliveryTime = ((count * avgDeliveryTime) + (other.count * other.avgDeliveryTime))/newCount;
    count           = newCount;
  }
}
