package com.neverwinterdp.sysinfo;

import java.io.Serializable;

@SuppressWarnings({"restriction", "serial"})
public class OS implements Serializable {
  private String name;
  private String arch ;
  private long   availableProcessor;
  private long   processCpuTime;
  private double processCpuLoad;
  private double systemCpuLoad;
  private double systemCpuLoadAverage;
  private long   freePhysicalMemorySize;
  private long   freeSwapSpaceSize;
  
  public OS(com.sun.management.OperatingSystemMXBean operatingSystemMXBean) {
    name                   = operatingSystemMXBean.getName();
    arch                   = operatingSystemMXBean.getArch();
    availableProcessor     = operatingSystemMXBean.getAvailableProcessors() ;
    processCpuTime         = operatingSystemMXBean.getProcessCpuTime();
    processCpuLoad         = operatingSystemMXBean.getProcessCpuLoad();
    systemCpuLoad          = operatingSystemMXBean.getSystemCpuLoad();
    systemCpuLoadAverage   = operatingSystemMXBean.getSystemLoadAverage();
    freePhysicalMemorySize = operatingSystemMXBean.getFreePhysicalMemorySize() ;
    freeSwapSpaceSize      = operatingSystemMXBean.getFreeSwapSpaceSize() ;
  }
  
  public String getName() { return name; }
  public void setName(String name) { this.name = name; }

  public String getArch() { return arch; }
  public void setArch(String arch) { this.arch = arch; }

  public long getAvailableProcessor() { return availableProcessor; }
  public void setAvailableProcessor(long availableProcessor) { this.availableProcessor = availableProcessor; }

  public long getProcessCpuTime() { return processCpuTime; }
  public void setProcessCpuTime(long processCpuTime) { this.processCpuTime = processCpuTime; }

  public double getProcessCpuLoad() { return processCpuLoad; }
  public void setProcessCpuLoad(double processCpuLoad) { this.processCpuLoad = processCpuLoad; }

  public double getSystemCpuLoad() { return systemCpuLoad; }
  public void setSystemCpuLoad(double systemCpuLoad) { this.systemCpuLoad = systemCpuLoad; }

  public double getSystemCpuLoadAverage() { return systemCpuLoadAverage; }
  public void setSystemCpuLoadAverage(double systemCpuLoadAverage) { this.systemCpuLoadAverage = systemCpuLoadAverage; }

  public long getFreePhysicalMemorySize() { return freePhysicalMemorySize; }
  public void setFreePhysicalMemorySize(long freePhysicalMemorySize) {
    this.freePhysicalMemorySize = freePhysicalMemorySize;
  }

  public long getFreeSwapSpaceSize() { return freeSwapSpaceSize; }
  public void setFreeSwapSpaceSize(long freeSwapSpaceSize) { this.freeSwapSpaceSize = freeSwapSpaceSize; }
}
