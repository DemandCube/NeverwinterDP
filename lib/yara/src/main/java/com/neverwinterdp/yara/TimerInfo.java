package com.neverwinterdp.yara;

import java.io.Serializable;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.neverwinterdp.util.text.DateUtil;

public class TimerInfo implements Serializable {
  @JsonFormat(shape=JsonFormat.Shape.STRING, pattern="dd/MM/yyyy HH:mm:ss")
  private Date   timestamp ;
  private String serverName;
  private String name;
  private long   count;
  private long   min;
  private long   max;
  private double mean;
  private double stdDev;
  private double quantile75;
  private double quantile90;
  private double quantile95;
  private double quantile99;
  private double quantile99999;
  private double oneMinRate;
  private double fiveMinRate;
  private double fifteenMinRate;
  private double meanRate;

  public TimerInfo() {
  }
  
  public TimerInfo(String serverName, Timer timer) {
    timestamp = new Date();
    this.serverName = serverName;
    name = timer.getName();
    count = timer.getCount();
    min = timer.getHistogram().getMin();
    max = timer.getHistogram().getMax();
    mean = timer.getMeanRate();
    stdDev = timer.getHistogram().getStdDev();
    quantile75 = timer.getHistogram().getQuantile(.75);
    quantile90 = timer.getHistogram().getQuantile(.90);
    quantile95 = timer.getHistogram().getQuantile(.95);
    quantile99 = timer.getHistogram().getQuantile(.75);
    quantile99999 = timer.getHistogram().getQuantile(.99999);
    oneMinRate = timer.getOneMinuteRate();
    fiveMinRate = timer.getFiveMinuteRate();
    fifteenMinRate = timer.getFifteenMinuteRate();
    meanRate = timer.getMeanRate();
  }

  public String uniqueId() { 
    return "host=" + serverName + ",timestamp=" + DateUtil.asCompactDateTimeId(timestamp); 
  }
  
  public Date getTimestamp() { return timestamp; }
  public void setTimestamp(Date timestamp) { this.timestamp = timestamp; }
  
  public String getServerName() { return serverName; }
  public void setServerName(String serverName) { this.serverName = serverName; }

  public String getName() { return name; }
  public void setName(String name) { this.name = name; }

  public long getCount() { return count; }
  public void setCount(long count) { this.count = count; }

  public long getMin() { return min; }
  public void setMin(long min) { this.min = min; }

  public long getMax() { return max; }
  public void setMax(long max) { this.max = max; }

  public double getMean() { return mean; }
  public void setMean(double mean) { this.mean = mean; }

  public double getStdDev() { return stdDev; }
  public void setStdDev(double stdDev) { this.stdDev = stdDev; }

  public double getQuantile75() { return quantile75; }
  public void setQuantile75(double quantile75) {  this.quantile75 = quantile75; }

  public double getQuantile90() { return quantile90; }
  public void setQuantile90(double quantile90) { this.quantile90 = quantile90; }

  public double getQuantile95() { return quantile95; }
  public void setQuantile95(double quantile95) { this.quantile95 = quantile95; }

  public double getQuantile99() { return quantile99; }
  public void setQuantile99(double quantile99) { this.quantile99 = quantile99; }

  public double getQuantile99999() { return quantile99999; }
  public void setQuantile99999(double quantile99999) { this.quantile99999 = quantile99999; }

  public double getOneMinRate() { return oneMinRate; }
  public void setOneMinRate(double oneMinRate) { this.oneMinRate = oneMinRate; }

  public double getFiveMinRate() {  return fiveMinRate; }
  public void setFiveMinRate(double fiveMinRate) { this.fiveMinRate = fiveMinRate; }

  public double getFifteenMinRate() { return fifteenMinRate; }
  public void setFifteenMinRate(double fifteenMinRate) { this.fifteenMinRate = fifteenMinRate; }

  public double getMeanRate() { return meanRate; }
  public void setMeanRate(double meanRate) { this.meanRate = meanRate; }
}