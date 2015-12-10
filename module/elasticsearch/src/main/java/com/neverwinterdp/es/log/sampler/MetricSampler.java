package com.neverwinterdp.es.log.sampler;

import java.util.Random;

import com.beust.jcommander.JCommander;
import com.neverwinterdp.es.log.ObjectLoggerService;
import com.neverwinterdp.monitor.jhiccup.JHiccupInfo;
import com.neverwinterdp.monitor.jhiccup.JHiccupMeter;
import com.neverwinterdp.os.ClassLoadedInfo;
import com.neverwinterdp.os.FileStoreInfo;
import com.neverwinterdp.os.GCInfo;
import com.neverwinterdp.os.MemoryInfo;
import com.neverwinterdp.os.OSInfo;
import com.neverwinterdp.os.OSManagement;
import com.neverwinterdp.os.RuntimeEnv;
import com.neverwinterdp.os.ThreadCountInfo;

public class MetricSampler {
  static public void main(String[] args) throws Exception {
    MetricSamplerConfig config = new MetricSamplerConfig();
    new JCommander(config, args);
    RuntimeEnv runtimeEnv = new RuntimeEnv(config.vmName, config.vmName, config.appDir);
    OSManagement osMan = new OSManagement(runtimeEnv);
    JHiccupMeter hiccupMeter =  new JHiccupMeter(runtimeEnv.getVMName(), 1l/*resolutionMs*/);
    
    
    String bufferDir = runtimeEnv.getAppDir() + config.bufferDir;
    ObjectLoggerService service = new ObjectLoggerService(new String[] { config.esConnect }, bufferDir, 25000);
    service.add(FileStoreInfo.class,   "neverwinterdp-monitor-storage");
    service.add(GCInfo.class,          "neverwinterdp-monitor-gc");
    service.add(MemoryInfo.class,      "neverwinterdp-monitor-memory");
    service.add(OSInfo.class,          "neverwinterdp-monitor-os");
    service.add(ThreadCountInfo.class, "neverwinterdp-monitor-thread");
    service.add(ClassLoadedInfo.class, "neverwinterdp-monitor-classloader");
    service.add(JHiccupInfo.class, "neverwinterdp-monitor-hiccup");
    
    Random r = new Random();
    while (true) {
      int fromBytes = 512;
      int toBytes = 1024;
      byte data[] = new byte[r.nextInt(toBytes-fromBytes) + fromBytes];

      GCInfo[] gcinfos = osMan.getGCInfo();
      MemoryInfo[] memoryInfos = osMan.getMemoryInfo();
      ThreadCountInfo threadCountInfos = osMan.getThreadCountInfo();
      ClassLoadedInfo classLoadedInfo = osMan.getLoadedClassInfo();
      OSInfo osInfo = osMan.getOSInfo();
      JHiccupInfo hiccupInfo = hiccupMeter.getHiccupInfo();
      
      service.log(threadCountInfos.uniqueId(), threadCountInfos);
      service.log(classLoadedInfo.uniqueId(), classLoadedInfo);
      service.log(osInfo.uniqueId(), osInfo);
      service.log(hiccupInfo.uniqueId(), hiccupInfo);
      
      for (GCInfo sel : gcinfos) {
        if (sel != null) {
          service.log(sel.uniqueId(), sel);
        }
      }

      for (MemoryInfo sel : memoryInfos) {
        service.log(sel.uniqueId(), sel);
      }

      System.out.println(OSInfo.getFormattedText(osInfo));
      System.out.println(MemoryInfo.getFormattedText(memoryInfos));
      System.out.println(GCInfo.getFormattedText(gcinfos));
      System.out.println(ThreadCountInfo.getFormattedText(threadCountInfos));
      System.out.println(ClassLoadedInfo.getFormattedText(classLoadedInfo));
      System.out.println(JHiccupInfo.getFormattedText(hiccupInfo));
      Thread.sleep(5000);
    }
  }
}
