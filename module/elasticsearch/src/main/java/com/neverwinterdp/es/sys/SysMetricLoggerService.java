package com.neverwinterdp.es.sys;

import java.io.IOException;
import java.util.Date;

import javax.annotation.PreDestroy;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.es.log.ObjectLoggerService;
import com.neverwinterdp.monitor.jhiccup.JHiccupMeter;
import com.neverwinterdp.os.OSManagement;
import com.neverwinterdp.os.RuntimeEnv;

@Singleton
public class SysMetricLoggerService extends ObjectLoggerService {
  private OSManagement              osManagement;
  private JHiccupMeter              jhiccupMetter;
  private MetricInfoCollectorThread metricCollectorThread;
  private long                      logPeriod         = 15000;
  
  @Inject
  public void onInit(RuntimeEnv runtimeEnv, OSManagement osManagement) throws Exception {
    this.osManagement = osManagement;
    //Detect only the hiccup that has more than 50ms to save the cpu cycle
    jhiccupMetter = new JHiccupMeter(runtimeEnv.getVMName(), 50L /*resolutionMs*/); 
    String bufferBaseDir = runtimeEnv.getDataDir() + "/buffer/sys-metric-log" ;
    String[] esConnect = { "elasticsearch-1:9300" };
    init(esConnect, bufferBaseDir, 25000);
   
    //add(DetailThreadInfo.class);
    add(SysMetric.class, "neverwinterdp-sys-metric");
    
    metricCollectorThread = new MetricInfoCollectorThread();
    metricCollectorThread.start();
  }
  
  @PreDestroy
  public void onDestroy() throws IOException {
    System.err.println("OSMonitorLoggerService: onDestroy.........................");
    metricCollectorThread.interrupt();
    close();
  }
  
  public void setLogPeriod(long period) { this.logPeriod = period; }
  
  public void log() {
    SysMetric sysMetric = new SysMetric();
    sysMetric.setTimestamp(new Date());
    sysMetric.setHost(osManagement.getVMName());
    sysMetric.add("storage", osManagement.getFileStoreInfo());
    sysMetric.add("gc",      osManagement.getGCInfo());
    sysMetric.add("mem",     osManagement.getMemoryInfo());
    sysMetric.add("os",      osManagement.getOSInfo());
    sysMetric.add("thread",  osManagement.getThreadCountInfo());
    sysMetric.add("jhicup",  jhiccupMetter.getHiccupInfo());
    addLog(sysMetric.uniqueId(), sysMetric);
  }
  
  public class MetricInfoCollectorThread extends Thread {
    public void run() {
      try {
        while(true) {
          log();
          Thread.sleep(logPeriod);
        }
      } catch(InterruptedException e) {
      } catch(Throwable t) {
        t.printStackTrace();
      }
    }
  }
}
