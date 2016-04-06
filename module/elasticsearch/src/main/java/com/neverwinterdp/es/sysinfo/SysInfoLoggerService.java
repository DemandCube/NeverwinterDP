package com.neverwinterdp.es.sysinfo;

import java.io.IOException;
import java.util.Date;

import javax.annotation.PreDestroy;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.es.log.ObjectLoggerService;
import com.neverwinterdp.monitor.jhiccup.JHiccupMeter;
import com.neverwinterdp.os.RuntimeEnv;
import com.neverwinterdp.sysinfo.SysInfoService;

@Singleton
public class SysInfoLoggerService extends ObjectLoggerService {
  private String                    serverName;
  private SysInfoService            sysInfoService;
  private JHiccupMeter              jhiccupMetter;
  private MetricInfoCollectorThread metricCollectorThread;
  private long                      logPeriod         = 15000;
  
  @Inject
  public void onInit(RuntimeEnv runtimeEnv) throws Exception {
    this.serverName     = runtimeEnv.getVMName();
    this.sysInfoService = new SysInfoService();
    //Detect only the hiccup that has more than 50ms to save the cpu cycle
    jhiccupMetter = new JHiccupMeter(runtimeEnv.getVMName(), 50L /*resolutionMs*/); 
    String bufferBaseDir = runtimeEnv.getDataDir() + "/buffer/sys-info" ;
    String[] esConnect = runtimeEnv.getEsConnects();
    init(esConnect, bufferBaseDir, 25000);
   
    add(SysInfo.class, "neverwinterdp-sys-info");
    
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
    SysInfo info = new SysInfo();
    info.setTimestamp(new Date());
    info.setHost(serverName);
    info.add("storage", sysInfoService.getFileStore());
    info.add("gc",      sysInfoService.getGC());
    info.add("mem",     sysInfoService.getMemory());
    info.add("os",      sysInfoService.getOS());
    info.add("thread",  sysInfoService.getThreadCount());
    info.add("jhicup",  jhiccupMetter.getHiccupInfo());
    addLog(info.uniqueId(), info);
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
