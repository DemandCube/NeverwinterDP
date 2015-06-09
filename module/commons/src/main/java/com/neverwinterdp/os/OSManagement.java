package com.neverwinterdp.os;

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.List;

import com.sun.management.OperatingSystemMXBean;

public class OSManagement {
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
  
  public ThreadCountInfo getThreadCountInfo() { 
    return new ThreadCountInfo(ManagementFactory.getThreadMXBean()); 
  }
  
  public DetailThreadInfo[] getDetailThreadInfo() {
    ThreadMXBean mbean = ManagementFactory.getThreadMXBean() ;
    long[] tid = mbean.getAllThreadIds() ;
    DetailThreadInfo[] detailThreadInfo = new DetailThreadInfo[tid.length] ;
    for(int i = 0; i < tid.length; i++) {
      ThreadInfo tinfo = mbean.getThreadInfo(tid[i], 10) ;
      detailThreadInfo[i] = new DetailThreadInfo(mbean, tinfo) ;
    }
    return detailThreadInfo;
  }
  
  public FileStoreInfo[] getFileStoreInfo() throws IOException {
    FileSystem fs = FileSystems.getDefault();
    List<FileStoreInfo> fsStoreInfo = new ArrayList<>();
    for (FileStore store: fs.getFileStores()) {
      fsStoreInfo.add(new FileStoreInfo(store));
    }
    return fsStoreInfo.toArray(new FileStoreInfo[fsStoreInfo.size()]);
  }
  
  public OSInfo getOSInfo() {
    return new OSInfo(ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class));
  }
}
