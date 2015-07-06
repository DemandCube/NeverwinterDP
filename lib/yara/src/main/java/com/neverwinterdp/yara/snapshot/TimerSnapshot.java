package com.neverwinterdp.yara.snapshot;

import java.io.Serializable;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.neverwinterdp.util.text.DateUtil;
import com.neverwinterdp.yara.Histogram;
import com.neverwinterdp.yara.Timer;

public class TimerSnapshot implements Serializable {
  @JsonFormat(shape=JsonFormat.Shape.STRING, pattern="dd/MM/yyyy HH:mm:ss")
  private Date   timestamp ;
  private String serverName = "NA";
  
  private String name;
  private long   count;
  private long   min;
  private long   max;
  private long   mean;
  private long   stddev;
  private long   p50;
  private long   p75;
  private long   p90;
  private long   p95;
  private long   p98;
  private long   p99;
  private long   p999;
  private double m1Rate;
  private double m5Rate;
  private double m15Rate;
  private double meanRate;
  private String durationUnit = "ns";
  private String rateUnit = "call/s";
  
  public TimerSnapshot() {
  }
  
  public TimerSnapshot(Timer timer, TimeUnit timeUnit) {
    this("NA", timer, timeUnit);
  }
  
  public TimerSnapshot(String serverName, Timer timer, TimeUnit timeUnit) {
    this.serverName = serverName;
    timestamp = new Date();
    name = timer.getName();
    Histogram histogram = timer.getHistogram();
    count = timer.getCount();
    min = timeUnit.convert(histogram.getMin(), TimeUnit.NANOSECONDS);
    max = timeUnit.convert(histogram.getMax(), TimeUnit.NANOSECONDS);
    mean = timeUnit.convert((long)histogram.getMean(), TimeUnit.NANOSECONDS);
    stddev = timeUnit.convert((long)histogram.getStdDev(), TimeUnit.NANOSECONDS);
    p50 = timeUnit.convert(histogram.getQuantile(0.50), TimeUnit.NANOSECONDS);
    p75 = timeUnit.convert(histogram.getQuantile(0.75), TimeUnit.NANOSECONDS);
    p90 = timeUnit.convert(histogram.getQuantile(0.90), TimeUnit.NANOSECONDS);
    p95 = timeUnit.convert(histogram.getQuantile(0.95), TimeUnit.NANOSECONDS);
    p98 = timeUnit.convert(histogram.getQuantile(0.98), TimeUnit.NANOSECONDS);
    p99 = timeUnit.convert(histogram.getQuantile(0.99), TimeUnit.NANOSECONDS);
    p999 = timeUnit.convert(histogram.getQuantile(0.999), TimeUnit.NANOSECONDS);
    m1Rate = timer.getOneMinuteRate();
    m5Rate = timer.getFiveMinuteRate();
    m15Rate = timer.getFifteenMinuteRate();
    meanRate = timer.getMeanRate();
    durationUnit = timeUnit.toString() ;
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

  public long getMean() { return mean; }
  public void setMean(long mean) { this.mean = mean; }

  public long getStddev() { return stddev; }
  public void setStddev(long stddev) { this.stddev = stddev; }

  public long getP50() { return p50; }
  public void setP50(long p50) { this.p50 = p50; }

  public long getP75() { return p75; }
  public void setP75(long p75) { this.p75 = p75; }

  public long getP90() { return p90; }
  public void setP90(long p90) { this.p90 = p90; }

  public long getP95() { return p95; }
  public void setP95(long p95) { this.p95 = p95; }

  public long getP98() { return p98; }
  public void setP98(long p98) { this.p98 = p98; }

  public long getP99() { return p99; }
  public void setP99(long p99) { this.p99 = p99; }

  public long getP999() { return p999; }
  public void setP999(long p999) { this.p999 = p999; }

  public double getM1Rate() { return m1Rate; }
  public void setM1Rate(double m1Rate) { this.m1Rate = m1Rate; }

  public double getM5Rate() { return m5Rate; }
  public void setM5Rate(double m5Rate) { this.m5Rate = m5Rate; }

  public double getM15Rate() { return m15Rate; }
  public void setM15Rate(double m15Rate) { this.m15Rate = m15Rate; }

  public double getMeanRate() { return meanRate; }
  public void setMeanRate(double meanRate) { this.meanRate = meanRate;  }

  public String getDurationUnit() {
    return durationUnit;
  }

  public void setDurationUnit(String durationUnit) {
    this.durationUnit = durationUnit;
  }

  public String getRateUnit() {
    return rateUnit;
  }

  public void setRateUnit(String rateUnit) {
    this.rateUnit = rateUnit;
  }
  
  
}