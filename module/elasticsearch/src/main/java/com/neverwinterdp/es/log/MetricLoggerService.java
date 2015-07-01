package com.neverwinterdp.es.log;

import java.util.Map;

import com.google.inject.Inject;
import com.neverwinterdp.yara.Counter;
import com.neverwinterdp.yara.MetricRegistry;
import com.neverwinterdp.yara.Timer;

public class MetricLoggerService  extends ObjectLoggerService {
  @Inject
  private MetricRegistry metricRegistry ;

  public void onInit() {
    Map<String, Counter> counters = metricRegistry.getCounters() ;
    Map<String, Timer>  timers = metricRegistry.getTimers();
    Timer timer1 = timers.get("1");
    
  }
}
