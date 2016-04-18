package com.neverwinterdp.monitor.jhiccup;

import java.io.Serializable;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.neverwinterdp.util.text.DateUtil;
import com.neverwinterdp.util.text.TabularFormater;

@SuppressWarnings("serial")
public class JHiccupInfo implements Serializable {
  @JsonFormat(shape=JsonFormat.Shape.STRING, pattern="dd/MM/yyyy HH:mm:ss")
  private Date   timestamp;
  private String host;
  
  private double maxValue;
  private double p99;
  private double p9990;
  private double p9999;
  private double totalMaxValue;

  public JHiccupInfo() {
    timestamp = new Date(System.currentTimeMillis()) ;
  }
  
  public String uniqueId() { 
    return "host=" + host + ",timestamp=" + DateUtil.asCompactDateTimeId(timestamp); 
  }
  
  public double getP99() { return p99; }
  public void setP99(double p99) { this.p99 = p99; }

  public double getP9990() { return p9990; }
  public void setP9990(double p9990) { this.p9990 = p9990; }

  public double getP9999() { return p9999; }
  public void setP9999(double p9999) { this.p9999 = p9999; }
  
  public double getMaxValue() { return maxValue; }
  public void setMaxValue(double maxValue) { this.maxValue = maxValue; }

  public double getTotalMaxValue() { return totalMaxValue; }
  public void setTotalMaxValue(double totalMaxValue) { this.totalMaxValue = totalMaxValue; }
  
  public Date getTimestamp() { return timestamp; }
  public void setTimestamp(Date timestamp) {  this.timestamp = timestamp; }
  
  public String getHost() { return host; }
  public void setHost(String host) { this.host = host; }
  
  static public String getFormattedText(JHiccupInfo ... clInfo) {
    String[] header = {"Host", "Timestamp", "Interval Max","Total Max", "99%", "99.90%", "99.99%"} ;
    TabularFormater formatter = new TabularFormater(header) ;
    for(JHiccupInfo sel : clInfo) {
      formatter.addRow(
          sel.getHost(),
          DateUtil.asCompactDateTime(sel.getTimestamp()),
          sel.getMaxValue(),
          sel.getTotalMaxValue(),
          sel.getP99(),
          sel.getP9990(),
          sel.getP9999());
    }
    return formatter.getFormattedText() ;
  }
}
