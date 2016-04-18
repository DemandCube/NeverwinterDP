package com.neverwinterdp.es.log;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.inject.Inject;
import com.neverwinterdp.os.OSManagement;
import com.neverwinterdp.os.RuntimeEnv;
import com.neverwinterdp.yara.Counter;
import com.neverwinterdp.yara.Meter;
import com.neverwinterdp.yara.MetricRegistry;
import com.neverwinterdp.yara.Timer;
import com.neverwinterdp.yara.snapshot.CounterSnapshot;
import com.neverwinterdp.yara.snapshot.MetterSnapshot;
import com.neverwinterdp.yara.snapshot.TimerSnapshot;

public class MetricLoggerService  extends ObjectLoggerService {
  
  @Inject
  private MetricRegistry            metricRegistry;
  private String                    serverName;
  private MetricInfoCollectorThread metricCollectorThread;

  public MetricLoggerService() {}
  
  public MetricLoggerService(MetricRegistry metricRegistry, String serverName, String bufferBaseDir, String[] esConnect) throws Exception {
    this.metricRegistry = metricRegistry;
    init(serverName, bufferBaseDir, esConnect);
  }
  
  @Inject
  public void onInit(RuntimeEnv runtimeEnv, OSManagement osManagement) throws Exception {
    serverName = runtimeEnv.getVMName();
    String bufferBaseDir = runtimeEnv.getDataDir() + "/buffer/metric-log" ;
    String[] esConnect = { "elasticsearch-1:9300" };
    init(serverName, bufferBaseDir, esConnect);
  }
  
  private void init(String serverName, String bufferBaseDir, String[] esConnect) throws Exception {
    init(esConnect, bufferBaseDir, 25000);
    add(CounterSnapshot.class, "neverwinterdp-metric-counter");
    add(TimerSnapshot.class,   "neverwinterdp-metric-timer");
    add(MetterSnapshot.class,  "neverwinterdp-metric-metter");
    
    metricCollectorThread = new MetricInfoCollectorThread();
    metricCollectorThread.start();
  }

  
  
  public class MetricInfoCollectorThread extends Thread {
    public void run() {
      try {
        while(true) {
          Map<String, Counter> counters = metricRegistry.getCounters() ;
          for(Map.Entry<String, Counter> entry : counters.entrySet()) {
            CounterSnapshot counterSnapshot = new CounterSnapshot(serverName, entry.getValue());
            addLog(counterSnapshot.uniqueId(), counterSnapshot);
          }
          Map<String, Timer>  timers = metricRegistry.getTimers();
          for(Map.Entry<String, Timer> entry : timers.entrySet()) {
            TimerSnapshot timerSnapshot = new TimerSnapshot(serverName, entry.getValue(), TimeUnit.MILLISECONDS);
            addLog(timerSnapshot.uniqueId(), timerSnapshot);
          }
          Map<String, Meter>  meters = metricRegistry.getMeters();
          for(Map.Entry<String, Meter> entry : meters.entrySet()) {
            MetterSnapshot meterSnapshot = new MetterSnapshot(serverName, entry.getValue());
            addLog(meterSnapshot.uniqueId(), meterSnapshot);
          }
          Thread.sleep(30000);
        }
      } catch (InterruptedException e) {
      }
    }
  }
}
