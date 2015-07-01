package com.neverwinterdp.yara;

public class TimerInfo {
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

  public String getServerName() { return serverName; }
  public void setServerName(String serverName) { this.serverName = serverName; }

  public String getName() { return name; }
  public void setName(String name) { this.name = name; }

  public long getCount() { return count; }
  public void setCount(long count) { this.count = count; }
}