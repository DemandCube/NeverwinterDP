package com.neverwinterdp.os;

import java.io.Serializable;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.neverwinterdp.util.text.DateUtil;
import com.neverwinterdp.util.text.TabularFormater;

@SuppressWarnings("serial")
public class IntervalHiccupInfo implements Serializable {
  @JsonFormat(shape=JsonFormat.Shape.STRING, pattern="dd/MM/yyyy HH:mm:ss")
  private Date   timestamp;
  private String host;
  private double maxValue;
  private double per_99;
  private double per_9990;
  private double per_9999;
  private double totalMaxValue;
 
  public double getPer_99() {
    return per_99;
  }

  public void setPer_99(double per_99) {
    this.per_99 = per_99;
  }

  public double getPer_9990() {
    return per_9990;
  }

  public void setPer_9990(double per_9990) {
    this.per_9990 = per_9990;
  }

  public double getPer_9999() {
    return per_9999;
  }

  public void setPer_9999(double per_9999) {
    this.per_9999 = per_9999;
  }
  
  public double getMaxValue() {
    return maxValue;
  }

  public void setMaxValue(double maxValue) {
    this.maxValue = maxValue;
  }

 

  public double getTotalMaxValue() {
    return totalMaxValue;
  }

  public void setTotalMaxValue(double totalMaxValue) {
    this.totalMaxValue = totalMaxValue;
  }

  public IntervalHiccupInfo() {
    timestamp = new Date(System.currentTimeMillis()) ;
  }
  
  public String uniqueId() { 
    return "host=" + host + ",timestamp=" + DateUtil.asCompactDateTimeId(timestamp); 
  }
  
  public Date getTimestamp() { return timestamp; }
  public void setTimestamp(Date timestamp) {  this.timestamp = timestamp; }
  
  public String getHost() { return host; }
  public void setHost(String host) { this.host = host; }
  
  static public String getFormattedText(IntervalHiccupInfo ... clInfo) {
    String[] header = {"Host", "Timestamp", "Interval Max","Total Max", "99%", "99.90%", "99.99%"} ;
    TabularFormater formatter = new TabularFormater(header) ;
    for(IntervalHiccupInfo sel : clInfo) {
      formatter.addRow(
          sel.getHost(),
          DateUtil.asCompactDateTime(sel.getTimestamp()),
          sel.getMaxValue(),
          sel.getTotalMaxValue(),
          sel.getPer_99(),
          sel.getPer_9990(),
          sel.getPer_9999());
    }
    return formatter.getFormattedText() ;
  }

  
}
