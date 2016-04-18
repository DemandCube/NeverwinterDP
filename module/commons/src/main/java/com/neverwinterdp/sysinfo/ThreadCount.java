package com.neverwinterdp.sysinfo;

import java.io.Serializable;
import java.lang.management.ThreadMXBean;

@SuppressWarnings("serial")
public class ThreadCount implements Serializable {
  private long   threadStartedCount;
  private long   peekThreadCount;
  private long   threadCount;
  private long   deamonThreadCount;

  public ThreadCount() { }

  public ThreadCount(ThreadMXBean mbean) {
    threadStartedCount = mbean.getTotalStartedThreadCount();
    peekThreadCount = mbean.getPeakThreadCount();
    threadCount = mbean.getThreadCount();
    deamonThreadCount = mbean.getDaemonThreadCount();
  }

  public long getThreadStartedCount() { return threadStartedCount; }
  public void setThreadStartedCount(long threadStartedCount) { this.threadStartedCount = threadStartedCount; }

  public long getPeekThreadCount() { return peekThreadCount; }
  public void setPeekThreadCount(long peekThreadCount) { this.peekThreadCount = peekThreadCount; }

  public long getThreadCount() { return threadCount; }
  public void setThreadCount(long threadCount) { this.threadCount = threadCount; }

  public long getDeamonThreadCount() { return deamonThreadCount; }
  public void setDeamonThreadCount(long deamonThreadCount) { this.deamonThreadCount = deamonThreadCount; }
}