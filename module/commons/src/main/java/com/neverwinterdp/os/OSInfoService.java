package com.neverwinterdp.os;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.List;

public class OSInfoService {
  public MemoryInfo[] getMemoryInfo() {
    MemoryMXBean mbean = ManagementFactory.getMemoryMXBean();
    MemoryInfo heapMemory = new MemoryInfo("Heap Memory", mbean.getHeapMemoryUsage());
    MemoryInfo nonHeapMemory = new MemoryInfo("Non Heap Memory", mbean.getNonHeapMemoryUsage());
    return new MemoryInfo[] { heapMemory, nonHeapMemory } ;
  }
  
  public GCInfo[] getGCInfo() {
    List<GarbageCollectorMXBean> gcbeans = ManagementFactory.getGarbageCollectorMXBeans() ; 
    GCInfo[] gcInfo = new GCInfo[gcbeans.size()];
    for(int i = 0; i < gcbeans.size(); i++) {
      GarbageCollectorMXBean gcbean = gcbeans.get(i) ;
      gcInfo[i] = new GCInfo(gcbean);
    }
    return gcInfo;
  }
}
