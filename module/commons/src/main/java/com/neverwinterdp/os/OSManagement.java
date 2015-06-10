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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.sun.management.OperatingSystemMXBean;

@Singleton
public class OSManagement {
  @Inject
  private RuntimeEnv runtimeEnv ;
  
  public OSManagement() { }
  
  public OSManagement(RuntimeEnv runtimeEnv) { 
    this.runtimeEnv = runtimeEnv ;
  }
  
  public MemoryInfo[] getMemoryInfo() {
    MemoryMXBean mbean = ManagementFactory.getMemoryMXBean();
    MemoryInfo heapMemory = new MemoryInfo("Heap Memory", mbean.getHeapMemoryUsage());
    heapMemory.setHost(runtimeEnv.getVMName());
    MemoryInfo nonHeapMemory = new MemoryInfo("Non Heap Memory", mbean.getNonHeapMemoryUsage());
    nonHeapMemory.setHost(runtimeEnv.getVMName());
    return new MemoryInfo[] { heapMemory, nonHeapMemory } ;
  }
  
  public GCInfo[] getGCInfo() {
    List<GarbageCollectorMXBean> gcbeans = ManagementFactory.getGarbageCollectorMXBeans() ; 
    GCInfo[] gcInfo = new GCInfo[gcbeans.size()];
    for(int i = 0; i < gcbeans.size(); i++) {
      GarbageCollectorMXBean gcbean = gcbeans.get(i) ;
      gcInfo[i] = new GCInfo(gcbean);
      gcInfo[i].setHost(runtimeEnv.getVMName());
    }
    return gcInfo;
  }
  
  public ThreadCountInfo getThreadCountInfo() { 
    ThreadCountInfo threadCountInfo = new ThreadCountInfo(ManagementFactory.getThreadMXBean()); 
    threadCountInfo.setHost(runtimeEnv.getVMName());
    return threadCountInfo;
  }
  
  public DetailThreadInfo[] getDetailThreadInfo() {
    ThreadMXBean mbean = ManagementFactory.getThreadMXBean() ;
    long[] tid = mbean.getAllThreadIds() ;
    List<DetailThreadInfo> holder = new ArrayList<>();
    for(int i = 0; i < tid.length; i++) {
      ThreadInfo tinfo = mbean.getThreadInfo(tid[i], 10) ;
      if(tinfo == null) continue ;
      DetailThreadInfo info = new DetailThreadInfo(mbean, tinfo) ;
      info.setHost(runtimeEnv.getVMName());
      holder.add(info);
    }
    DetailThreadInfo[] detailThreadInfo = new DetailThreadInfo[holder.size()] ;
    return holder.toArray(detailThreadInfo);
  }
  
  public FileStoreInfo[] getFileStoreInfo() {
    FileSystem fs = FileSystems.getDefault();
    List<FileStoreInfo> fsStoreInfo = new ArrayList<>();
    for (FileStore store: fs.getFileStores()) {
      try {
        FileStoreInfo info = new FileStoreInfo(store);
        info.setHost(runtimeEnv.getVMName());
        fsStoreInfo.add(info);
      } catch (IOException e) {
      }
    }
    return fsStoreInfo.toArray(new FileStoreInfo[fsStoreInfo.size()]);
  }
  
  public OSInfo getOSInfo() {
    OSInfo osInfo = new OSInfo(ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class));
    osInfo.setHost(runtimeEnv.getVMName());
    return osInfo;
  }
}
