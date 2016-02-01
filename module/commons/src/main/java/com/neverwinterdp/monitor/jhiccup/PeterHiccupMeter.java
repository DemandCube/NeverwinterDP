package com.neverwinterdp.monitor.jhiccup;

import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.SingleWriterRecorder;

import com.neverwinterdp.os.RuntimeEnv;

public class PeterHiccupMeter extends Thread {
  private String vmName          = "localhost";
  
  public double  resolutionMs    = 1.0;
  public long    startDelayMs    = 30000;
  public boolean allocateObjects = false;
  public String  errorMessage    = "";

  public Double outputValueUnitRatio = 1000000.0; // default to msec units for output.
  
  public PeterHiccupMeter(RuntimeEnv runtimeEnv) {
    setName("HiccupMeter");
    vmName = runtimeEnv.getVMName();
    setDaemon(true);
    start();
  }

  public class HiccupRecorder extends Thread {
    public volatile boolean doRun;
    private final boolean allocateObjects;
    public volatile Long lastSleepTimeObj; // public volatile to make sure
                                           // allocs are not optimized away...
    protected final SingleWriterRecorder recorder;

    public HiccupRecorder(final SingleWriterRecorder recorder, final boolean allocateObjects) {
      this.setDaemon(true);
      this.setName("HiccupRecorder");
      this.recorder = recorder;
      this.allocateObjects = allocateObjects;
      doRun = true;
    }

    public void terminate() {
      doRun = false;
    }

    public void run() {
      final long resolutionNsec = (long) (resolutionMs * 1000L * 1000L);
      try {
        long shortestObservedDeltaTimeNsec = Long.MAX_VALUE;
        while (doRun) {
          final long timeBeforeMeasurement = System.nanoTime();
          if (resolutionMs != 0) {
            TimeUnit.NANOSECONDS.sleep(resolutionNsec);
            if (allocateObjects) {
              // Allocate an object to make sure potential allocation stalls are
              // measured.
              lastSleepTimeObj = new Long(timeBeforeMeasurement);
            }
          }
          final long timeAfterMeasurement = System.nanoTime();
          final long deltaTimeNsec = timeAfterMeasurement - timeBeforeMeasurement;

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
  }

  public HiccupRecorder createHiccupRecorder(SingleWriterRecorder recorder) {
    return new HiccupRecorder(recorder, allocateObjects);
  }

  Histogram intervalHistogram = null;
  Histogram accumulatedHistogram = null;
  final SingleWriterRecorder recorder = new SingleWriterRecorder(1000L * 20L, 3600 * 1000L * 1000L * 1000L, 2);
  HiccupRecorder hiccupRecorder;

  @Override
  public void run() {
    final long uptimeAtInitialStartTime = ManagementFactory.getRuntimeMXBean().getUptime();
    long now = System.currentTimeMillis();
    long jvmStartTime = now - uptimeAtInitialStartTime;

    // Normal operating mode.
    // Launch a hiccup recorder, a process termination monitor, and an optional
    // control process:
    hiccupRecorder = createHiccupRecorder(recorder);

    try {
      if (startDelayMs > 0) {
        // Run hiccup recorder during startDelayMs time to let code warm up:
        while (startDelayMs > System.currentTimeMillis() - jvmStartTime) {
          Thread.sleep(100);
        }
        hiccupRecorder.terminate();
        hiccupRecorder.join();

        recorder.reset();
        hiccupRecorder = new HiccupRecorder(recorder, allocateObjects);
      }
      hiccupRecorder.start();
    } catch (InterruptedException e) {
      System.out.println("# HiccupMeter terminating...");
    }
  }

  JHiccupInfo hiccuoInfo;

  public JHiccupInfo getHiccupInfo() {
    hiccuoInfo = new JHiccupInfo();
    hiccuoInfo.setHost(vmName);
    // Get the latest interval histogram and give the recorder a fresh
    // Histogram for the next interval
    intervalHistogram = recorder.getIntervalHistogram(intervalHistogram);
    if (intervalHistogram.getTotalCount() > 0) {
      if (accumulatedHistogram == null && intervalHistogram != null) {
        accumulatedHistogram = ((Histogram) intervalHistogram).copy();
        accumulatedHistogram.reset();
        accumulatedHistogram.setAutoResize(true);
      }
      accumulatedHistogram.add(intervalHistogram);

      hiccuoInfo.setMaxValue(intervalHistogram.getMaxValue() / outputValueUnitRatio);
      hiccuoInfo.setTotalMaxValue(accumulatedHistogram.getMaxValue() / outputValueUnitRatio);
      hiccuoInfo.setP99(accumulatedHistogram.getValueAtPercentile(99.0) / outputValueUnitRatio);
      hiccuoInfo.setP9990(accumulatedHistogram.getValueAtPercentile(99.90) / outputValueUnitRatio);
      hiccuoInfo.setP9999(accumulatedHistogram.getValueAtPercentile(99.99) / outputValueUnitRatio);
    }
    return hiccuoInfo;
  }
}
