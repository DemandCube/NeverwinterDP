package com.neverwinterdp.sysinfo;

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.inject.Singleton;

@SuppressWarnings({"restriction"})
@Singleton
public class SysInfoService {
  
  private Map<String, GC> previousGCs = new HashMap<String, GC>() ;
  
  public Memory[] getMemory() {
    MemoryMXBean mbean = ManagementFactory.getMemoryMXBean();
    Memory heapMemory = new Memory("Heap", mbean.getHeapMemoryUsage());
    Memory nonHeapMemory = new Memory("NonHeap", mbean.getNonHeapMemoryUsage());
    return new Memory[] { heapMemory, nonHeapMemory } ;
  }
  
  
  public GC[] getGC() {
    List<GarbageCollectorMXBean> gcbeans = ManagementFactory.getGarbageCollectorMXBeans();
    GC[] gc = new GC[gcbeans.size()];
    for(int i = 0; i < gcbeans.size(); i++) {
      GarbageCollectorMXBean gcbean = gcbeans.get(i) ;
      gc[i] = new GC(gcbean);
      GC prevGCInfo = previousGCs.get(gc[i].getName());
      if(prevGCInfo != null) {
        gc[i].setDiffCollectionCount(gc[i].getCollectionCount() - prevGCInfo.getCollectionCount());
      }else{
        gc[i].setDiffCollectionCount(gc[i].getCollectionCount());
      }
      previousGCs.put(gc[i].getName(), gc[i]);
    }
    return gc;
  }
  
  public ThreadCount getThreadCount() { return new ThreadCount(ManagementFactory.getThreadMXBean());  }
  
  public LoadedClass getLoadedClass() { return new LoadedClass(ManagementFactory.getClassLoadingMXBean()); }
  
  public FileStore[] getFileStore() {
    FileSystem fs = FileSystems.getDefault();
    List<FileStore> fsStoreInfo = new ArrayList<>();
    for (java.nio.file.FileStore store: fs.getFileStores()) {
      try {
        FileStore info = new FileStore(store);
        fsStoreInfo.add(info);
      } catch (IOException e) {
      }
    }
    return fsStoreInfo.toArray(new FileStore[fsStoreInfo.size()]);
  }
  
  public OS getOS() {
    return new OS(ManagementFactory.getPlatformMXBean(com.sun.management.OperatingSystemMXBean.class));
  }
}