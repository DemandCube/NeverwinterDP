package com.neverwinterdp.es.log;

import java.io.IOException;

import javax.annotation.PreDestroy;

import com.google.inject.Inject;
import com.google.inject.Singleton;
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

@Singleton
public class OSMonitorLoggerService extends ObjectLoggerService {
  private OSManagement              osManagement;
  private JHiccupMeter              jhiccupMetter;
  private MetricInfoCollectorThread metricCollectorThread;
  
  @Inject
  public void onInit(RuntimeEnv runtimeEnv, OSManagement osManagement) throws Exception {
    this.osManagement = osManagement;
    //Detect only the hiccup that has more than 50ms to save the cpu cycle
    jhiccupMetter = new JHiccupMeter(runtimeEnv.getVMName(), 50L /*resolutionMs*/); 
    String bufferBaseDir = runtimeEnv.getDataDir() + "/buffer/os-monitor-log" ;
    String[] esConnect = runtimeEnv.getEsConnects();
    init(esConnect, bufferBaseDir, 25000);
   
    //add(DetailThreadInfo.class);
    add(FileStoreInfo.class,      "neverwinterdp-monitor-storage");
    add(GCInfo.class,             "neverwinterdp-monitor-gc");
    add(MemoryInfo.class,         "neverwinterdp-monitor-memory");
    add(OSInfo.class,             "neverwinterdp-monitor-os");
    add(ThreadCountInfo.class,    "neverwinterdp-monitor-thread");
    add(ClassLoadedInfo.class,    "neverwinterdp-monitor-classloader");
    add(JHiccupInfo.class,        "neverwinterdp-monitor-hiccup");
    
    metricCollectorThread = new MetricInfoCollectorThread();
    metricCollectorThread.start();
  }
  
  @PreDestroy
  public void onDestroy() throws IOException {
    System.err.println("OSMonitorLoggerService: onDestroy.........................");
    metricCollectorThread.interrupt();
    close();
  }
  
  public class MetricInfoCollectorThread extends Thread {
    public void run() {
      try {
        while(true) {
          for(FileStoreInfo sel : osManagement.getFileStoreInfo()) {
            addLog(sel.uniqueId(), sel);
          }

          for(GCInfo sel : osManagement.getGCInfo()) {
            addLog(sel.uniqueId(), sel);
          }

          for(MemoryInfo sel : osManagement.getMemoryInfo()) {
            addLog(sel.uniqueId(), sel);
          }

          OSInfo osInfo = osManagement.getOSInfo();
          addLog(osInfo.uniqueId(), osInfo);

          ThreadCountInfo threadCountInfo = osManagement.getThreadCountInfo();
          addLog(threadCountInfo.uniqueId(), threadCountInfo);
          
          JHiccupInfo jhiccupInfo = jhiccupMetter.getHiccupInfo();
          addLog(jhiccupInfo.uniqueId(), jhiccupInfo);
          
          Thread.sleep(15000);
        }
      } catch(InterruptedException e) {
      } catch(Throwable t) {
        t.printStackTrace();
      }
    }
  }
}
