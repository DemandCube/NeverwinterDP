package com.neverwinterdp.yara.snapshot;

import java.io.Serializable;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.neverwinterdp.util.text.DateUtil;
import com.neverwinterdp.yara.Meter;

public class MetterSnapshot implements Serializable {
  @JsonFormat(shape=JsonFormat.Shape.STRING, pattern="dd/MM/yyyy HH:mm:ss")
  private Date   timestamp;
  private String serverName;
  
  private String name ;
  private long   count;
  private String unit ;
  private double m1Rate;
  private double m5Rate;
  private double m15Rate;
  private double meanRate;
  
  public MetterSnapshot() {
  }

  public MetterSnapshot(String serverName, Meter meter) {
    timestamp = new Date();
    this.serverName = serverName;
    
    name  = meter.getName();
    count = meter.getCount();
    unit = meter.getUnit();
    m1Rate = meter.getOneMinuteRate();
    m5Rate = meter.getFiveMinuteRate();
    m15Rate = meter.getFifteenMinuteRate();
    meanRate = meter.getMeanRate();
  }
  
  public String uniqueId() { 
    return "host=" + serverName + ", name=" + name + ",timestamp=" + DateUtil.asCompactDateTimeId(timestamp); 
  }
  
  public Date getTimestamp() { return timestamp; }
  public void setTimestamp(Date timestamp) { this.timestamp = timestamp; }

  public String getServerName() { return serverName; }
  public void setServerName(String serverName) { this.serverName = serverName; }

  public String getName() { return name; }
  public void setName(String name) {
    this.name = name;
  }

  public long getCount() { return count; }
  public void setCount(long count) { this.count = count; }

  public String getUnit() { return unit; }
  public void setUnit(String unit) { this.unit = unit; }

  public double getM1Rate() { return m1Rate; }
  public void setM1Rate(double m1Rate) { this.m1Rate = m1Rate; }

  public double getM5Rate() { return m5Rate; }
  public void setM5Rate(double m5Rate) { this.m5Rate = m5Rate; }

  public double getM15Rate() { return m15Rate; }
  public void setM15Rate(double m15Rate) {  this.m15Rate = m15Rate; }

  public double getMeanRate() { return meanRate; }
  public void setMeanRate(double meanRate) { this.meanRate = meanRate; }
}