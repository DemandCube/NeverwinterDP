package com.neverwinterdp.os;

import java.io.Serializable;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.neverwinterdp.util.text.ByteUtil;
import com.neverwinterdp.util.text.DateUtil;
import com.neverwinterdp.util.text.TabularFormater;


@SuppressWarnings("serial")
public class OSInfo implements Serializable {
  @JsonFormat(shape=JsonFormat.Shape.STRING, pattern="dd/MM/yyyy HH:mm:ss")
  private Date   timestamp;
  private String host ;
  private String name;
  private String arch ;
  private long   availableProcessor;
  private long   processCpuTime;
  private double processCpuLoad;
  private double systemCpuLoad;
  private double systemCpuLoadAverage;
  private long   freePhysicalMemorySize;
  private long   freeSwapSpaceSize;
  
  public OSInfo(com.sun.management.OperatingSystemMXBean osMBean) {
    timestamp              = new Date();
    name                   = osMBean.getName();
    arch                   = osMBean.getArch();
    availableProcessor     = osMBean.getAvailableProcessors() ;
    processCpuTime         = osMBean.getProcessCpuTime();
    processCpuLoad         = osMBean.getProcessCpuLoad();
    systemCpuLoad          = osMBean.getSystemCpuLoad();
    systemCpuLoadAverage   = osMBean.getSystemLoadAverage();
    freePhysicalMemorySize = osMBean.getFreePhysicalMemorySize() ;
    freeSwapSpaceSize      = osMBean.getFreeSwapSpaceSize() ;
  }
  
  public String uniqueId() { 
    return "host=" + host + ",timestamp=" + DateUtil.asCompactDateTimeId(timestamp); 
  }
  
  public Date getTimestamp() { return timestamp; }
  public void setTimestamp(Date timestamp) { this.timestamp = timestamp; }
  
  public String getHost() { return host; }
  public void setHost(String host) { this.host = host; }

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
  
  public String getFormattedText() {
    return getFormattedText(this) ;
  }
  
  static public String getFormattedText(OSInfo ... info) {
    String[] header = {
      "Timestamp", "Host", "Name", "Arch", "# Core", "Proc Cpu Time", "Proc Cpu Load", 
      "Sys Cpu Load", "Sys Cpu Load Avg", "Free Mem", "Free Swap Mem"
    };
    TabularFormater formatter = new TabularFormater(header) ;
    for(OSInfo sel : info) {
      formatter.addRow(
          DateUtil.asCompactDateTime(sel.getTimestamp()),
          sel.getHost(),
          sel.getName(),
          sel.getArch(),
          sel.getAvailableProcessor(),
          DateUtil.timeNanoToHumanReadable(sel.getProcessCpuTime()),
          sel.getProcessCpuLoad(),
          sel.getSystemCpuLoad(),
          sel.getSystemCpuLoadAverage(),
          ByteUtil.byteToHumanReadable(sel.getFreePhysicalMemorySize()),
          ByteUtil.byteToHumanReadable(sel.getFreeSwapSpaceSize()));
    }
    return formatter.getFormattedText() ;
  }
}
