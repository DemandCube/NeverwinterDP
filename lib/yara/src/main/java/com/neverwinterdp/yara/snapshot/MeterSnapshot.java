package com.neverwinterdp.yara.snapshot;

import java.io.Serializable;

import com.neverwinterdp.yara.Meter;

public class MeterSnapshot implements Serializable {
  private long   count;
  private double m1Rate;
  private double m5Rate;
  private double m15Rate;
  private double meanRate;
  
  public MeterSnapshot() {
  }

  public MeterSnapshot(Meter meter) {
    count = meter.getCount();
    m1Rate = meter.getOneMinuteRate();
    m5Rate = meter.getFiveMinuteRate();
    m15Rate = meter.getFifteenMinuteRate();
    meanRate = meter.getMeanRate();
  }

  public long getCount() { return count; }

  public double getM1Rate() { return m1Rate; }

  public double getM5Rate() { return m5Rate; }

  public double getM15Rate() { return m15Rate; }

  public double getMeanRate() { return meanRate; }
}