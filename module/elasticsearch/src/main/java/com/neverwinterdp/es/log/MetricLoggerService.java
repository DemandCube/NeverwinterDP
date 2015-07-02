package com.neverwinterdp.es.log;

import java.util.Map;

import com.google.inject.Inject;
import com.neverwinterdp.es.log.OSMonitorLoggerService.MetricInfoCollectorThread;
import com.neverwinterdp.os.OSManagement;
import com.neverwinterdp.os.RuntimeEnv;
import com.neverwinterdp.yara.Counter;
import com.neverwinterdp.yara.CounterInfo;
import com.neverwinterdp.yara.MetricRegistry;
import com.neverwinterdp.yara.Timer;
import com.neverwinterdp.yara.TimerInfo;

public class MetricLoggerService  extends ObjectLoggerService {
  @Inject
  private MetricRegistry metricRegistry ;
  private String serverName;
  private MetricInfoCollectorThread metricCollectorThread;

  @Inject
  public void onInit(RuntimeEnv runtimeEnv, OSManagement osManagement) throws Exception {
    serverName = runtimeEnv.getVMName();
    String bufferBaseDir = runtimeEnv.getDataDir() + "/buffer/metric-log" ;
    String[] esConnect = { "elasticsearch-1:9300" };
    init(esConnect, bufferBaseDir, 25000);
   
    add(CounterInfo.class);
    add(TimerInfo.class);
    
    metricCollectorThread = new MetricInfoCollectorThread();
    metricCollectorThread.start();
  }

  public class MetricInfoCollectorThread extends Thread {
    public void run() {
      try {
        while(true) {
          Map<String, Counter> counters = metricRegistry.getCounters() ;
          for(Map.Entry<String, Counter> entry : counters.entrySet()) {
            CounterInfo counterInfo = new CounterInfo(serverName, entry.getValue());
            log(counterInfo.uniqueId(), counterInfo);
          }
          Map<String, Timer>  timers = metricRegistry.getTimers();
          for(Map.Entry<String, Timer> entry : timers.entrySet()) {
            TimerInfo timerInfo = new TimerInfo(serverName, entry.getValue());
            log(timerInfo.uniqueId(), timerInfo);
          }
          Thread.sleep(30000);
        }
      } catch (InterruptedException e) {
      }
    }
  }
}
