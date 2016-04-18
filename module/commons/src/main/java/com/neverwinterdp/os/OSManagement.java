package com.neverwinterdp.os;

import java.io.IOException;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.inject.Inject;
import com.google.inject.Singleton;

@SuppressWarnings({"restriction"})
@Singleton
public class OSManagement {
  private String vmName = "localhost";
  private Map<String, GCInfo> previousGCInfos = new HashMap<String, GCInfo>() ;
  
  public OSManagement() { }
  
  public OSManagement(RuntimeEnv runtimeEnv) { 
    this.vmName = runtimeEnv.getVMName() ;
  }
  
  @Inject
  public void onInject(RuntimeEnv runtimeEnv ) {
    vmName = runtimeEnv.getVMName();
  }
 
  public String getVMName() { return this.vmName; }
  
  public MemoryInfo[] getMemoryInfo() {
    MemoryMXBean mbean = ManagementFactory.getMemoryMXBean();
    MemoryInfo heapMemory = new MemoryInfo("Heap_Memory", mbean.getHeapMemoryUsage());
    heapMemory.setHost(vmName);
    MemoryInfo nonHeapMemory = new MemoryInfo("Non_Heap_Memory", mbean.getNonHeapMemoryUsage());
    nonHeapMemory.setHost(vmName);
    Iterator<MemoryPoolMXBean> memoryPoolMXBeans=ManagementFactory.getMemoryPoolMXBeans().iterator();
    while (memoryPoolMXBeans.hasNext()) {
      MemoryPoolMXBean memoryPoolMXBean=(MemoryPoolMXBean)memoryPoolMXBeans.next();
      MemoryUsage usage= memoryPoolMXBean.getPeakUsage();
      if(memoryPoolMXBean.getName().equals("PS Eden Space")){
        heapMemory.setUsedPSEdenSPace(usage.getUsed());
      } else if(memoryPoolMXBean.getName().equals("PS Survivor Space")){
        heapMemory.setUsedPSSurvivorSpace(usage.getUsed());
      } else if(memoryPoolMXBean.getName().equals("PS Old Gen")){
        heapMemory.setUsedPSOldGen(usage.getUsed());
      } else if(memoryPoolMXBean.getName().equals("Code Cache")){
        nonHeapMemory.setUsedCodeCashe(usage.getUsed());
      } else if(memoryPoolMXBean.getName().equals("Metaspace")){
        nonHeapMemory.setUsedMetaspace(usage.getUsed());
      } else if(memoryPoolMXBean.getName().equals("Compressed Class Space")){
        nonHeapMemory.setUsedCompressedClassSpace(usage.getUsed());
      }
    }
    return new MemoryInfo[] { heapMemory, nonHeapMemory } ;
  }
  public String getMemoryInfoFormattedText() { return MemoryInfo.getFormattedText(getMemoryInfo()); }
  
  
  public GCInfo[] getGCInfo() {
    List<GarbageCollectorMXBean> gcbeans = ManagementFactory.getGarbageCollectorMXBeans();
    GCInfo[] gcInfo = new GCInfo[gcbeans.size()];
    for(int i = 0; i < gcbeans.size(); i++) {
      GarbageCollectorMXBean gcbean = gcbeans.get(i) ;
      gcInfo[i] = new GCInfo(gcbean);
      GCInfo prevGCInfo = previousGCInfos.get(gcInfo[i].getName());
      if(prevGCInfo != null) {
        gcInfo[i].setDiffCollectionCount(gcInfo[i].getCollectionCount() - prevGCInfo.getCollectionCount());
      }else{
        gcInfo[i].setDiffCollectionCount(gcInfo[i].getCollectionCount());
      }
      gcInfo[i].setHost(vmName);
      previousGCInfos.put(gcInfo[i].getName(), gcInfo[i]);
    }
    return gcInfo;
  }
  
  public String getGCInfoFormattedText() { return GCInfo.getFormattedText(getGCInfo()); }
  
  public ThreadCountInfo getThreadCountInfo() { 
    ThreadCountInfo threadCountInfo = new ThreadCountInfo(ManagementFactory.getThreadMXBean()); 
    threadCountInfo.setHost(vmName);
    return threadCountInfo;
  }
  
  public ClassLoadedInfo getLoadedClassInfo() { 
    ClassLoadedInfo classLoadedInfo = new ClassLoadedInfo(ManagementFactory.getClassLoadingMXBean()); 
    classLoadedInfo.setHost(vmName);
    return classLoadedInfo;
  }
  
  public DetailThreadInfo[] getDetailThreadInfo() {
    ThreadMXBean mbean = ManagementFactory.getThreadMXBean() ;
    long[] tid = mbean.getAllThreadIds() ;
    List<DetailThreadInfo> holder = new ArrayList<>();
    for(int i = 0; i < tid.length; i++) {
      ThreadInfo tinfo = mbean.getThreadInfo(tid[i], 10) ;
      if(tinfo == null) continue ;
      DetailThreadInfo info = new DetailThreadInfo(mbean, tinfo) ;
      info.setHost(vmName);
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
        info.setHost(vmName);
        fsStoreInfo.add(info);
      } catch (IOException e) {
      }
    }
    return fsStoreInfo.toArray(new FileStoreInfo[fsStoreInfo.size()]);
  }
  
  public OSInfo getOSInfo() {
    
    OSInfo osInfo = new OSInfo(ManagementFactory.getPlatformMXBean(com.sun.management.OperatingSystemMXBean.class));
    
    osInfo.setHost(vmName);
    return osInfo;
  }
  
  public String getOSInfoFormattedText() {
    return OSInfo.getFormattedText(getOSInfo());
  }
}