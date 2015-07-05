package com.neverwinterdp.yara.snapshot;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import com.neverwinterdp.yara.EWMAMeter;

public class MeterSnapshot implements Serializable {
  private long   count;
  private double m1Rate;
  private double m5Rate;
  private double m15Rate;
  private double meanRate;
  
  public MeterSnapshot() {
  }

  public MeterSnapshot(EWMAMeter eWMAMeter, TimeUnit timeUnit) {
    count = eWMAMeter.getCount();
    m1Rate = eWMAMeter.getOneMinuteRate();
    m5Rate = eWMAMeter.getFiveMinuteRate();
    m15Rate = eWMAMeter.getFifteenMinuteRate();
    meanRate = eWMAMeter.getMeanRate();
  }

  public long getCount() { return count; }

  public double getM1Rate() { return m1Rate; }

  public double getM5Rate() { return m5Rate; }

  public double getM15Rate() { return m15Rate; }

  public double getMeanRate() { return meanRate; }
}