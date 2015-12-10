package com.neverwinterdp.monitor.jhiccup;

import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.SingleWriterRecorder;

public class JHiccupMeter {
  JHiccupRecorder jHiccupRecorder;

  public JHiccupMeter(String host, long resolutionMs) {
    jHiccupRecorder = new JHiccupRecorder(host, resolutionMs);
    jHiccupRecorder.start();
  }

  
  static public class JHiccupRecorder extends Thread {
    private String vmName   = "localhost";
    private boolean doRun   = true;
    protected SingleWriterRecorder recorder = new SingleWriterRecorder(1000L * 20L, 3600 * 1000L * 1000L * 1000L, 2);
    private  Histogram intervalHistogram = null;
    private  Histogram accumulatedHistogram = null;
    private  long      resolutionMs ;
    
    public JHiccupRecorder(String vmName, long resolutionMs) {
      this.vmName = vmName;
      this.resolutionMs = resolutionMs;
      setName("JHiccupRecorder");
      doRun = true;
    }

    public void terminate() { doRun = false; }

    public void run() {
      long resolutionNsec = TimeUnit.MILLISECONDS.toNanos(resolutionMs);
      System.out.println("resolutionNsec = " + resolutionNsec);
      try {
        long shortestObservedDeltaTimeNsec = Long.MAX_VALUE;
        while (doRun) {
          final long timeBeforeMeasurement = System.nanoTime();
          TimeUnit.NANOSECONDS.sleep(resolutionNsec);
          //Allocate an object to make sure potential allocation stalls are measured.
          Long lastSleepTimeObj = new Long(timeBeforeMeasurement);

          long timeAfterMeasurement = System.nanoTime();
          long deltaTimeNsec = timeAfterMeasurement - timeBeforeMeasurement;

          if (deltaTimeNsec < shortestObservedDeltaTimeNsec) {
            shortestObservedDeltaTimeNsec = deltaTimeNsec;
          }

          long hiccupTimeNsec = deltaTimeNsec - shortestObservedDeltaTimeNsec;
          recorder.recordValueWithExpectedInterval(hiccupTimeNsec, resolutionNsec);
        }
      } catch (InterruptedException e) {
        System.out.println("# HiccupRecorder interrupted/terminating...");
      }
    }
    
    final public JHiccupInfo getHiccupInfo() {
      JHiccupInfo hiccuoInfo = new JHiccupInfo();
      hiccuoInfo.setHost(vmName);
      //Get the latest interval histogram and give the recorder a fresh Histogram for the next interval
      intervalHistogram = recorder.getIntervalHistogram(intervalHistogram);
      if(intervalHistogram.getTotalCount() > 0) {
        if (accumulatedHistogram == null && intervalHistogram != null) {
          accumulatedHistogram = ((Histogram) intervalHistogram).copy();
          accumulatedHistogram.reset();
          accumulatedHistogram.setAutoResize(true);
        } else {
          accumulatedHistogram.add(intervalHistogram);
        }
        TimeUnit nanosc = TimeUnit.NANOSECONDS;
        hiccuoInfo.setMaxValue(nanosc.toMillis(intervalHistogram.getMaxValue()));
        hiccuoInfo.setTotalMaxValue(nanosc.toMillis(accumulatedHistogram.getMaxValue()));
        hiccuoInfo.setP99(nanosc.toMillis(accumulatedHistogram.getValueAtPercentile(99.0)));
        hiccuoInfo.setP9990(nanosc.toMillis(accumulatedHistogram.getValueAtPercentile(99.90)));
        hiccuoInfo.setP9999(nanosc.toMillis(accumulatedHistogram.getValueAtPercentile(99.99)));
      }
      return hiccuoInfo;
    }
  }

  final public JHiccupInfo getHiccupInfo() { return jHiccupRecorder.getHiccupInfo() ; }
}
