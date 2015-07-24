package com.neverwinterdp.yara.snapshot;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import com.neverwinterdp.util.text.TabularFormater;
import com.neverwinterdp.yara.cluster.ClusterCounter;
import com.neverwinterdp.yara.cluster.ClusterMetricRegistry;
import com.neverwinterdp.yara.cluster.ClusterTimer;

public class ClusterMetricRegistrySnapshot implements Serializable {
  private String clusterName; 
  private Map<String, ClusterCounterSnapshot> counters = new TreeMap<String,  ClusterCounterSnapshot>();
  private Map<String, ClusterTimerSnapshot>   timers   = new TreeMap<String,  ClusterTimerSnapshot>();
  private Map<String, ClusterMetterSnapshot>  metters   = new TreeMap<String, ClusterMetterSnapshot>();
  
  public ClusterMetricRegistrySnapshot() { }
  
  public ClusterMetricRegistrySnapshot(ClusterMetricRegistry registry) {
    this(registry, TimeUnit.MILLISECONDS) ;
  }
  
  public ClusterMetricRegistrySnapshot(ClusterMetricRegistry registry, TimeUnit timeUnit) {
    if(registry == null) return  ;
    for(Map.Entry<String, ClusterCounter> entry : registry.getCounters().entrySet()) {
      String key = entry.getKey() ;
      counters.put(key, new ClusterCounterSnapshot(key, entry.getValue())) ;
    }
    
    for(Map.Entry<String, ClusterTimer> entry : registry.getTimers().entrySet()) {
      timers.put(entry.getKey(), new ClusterTimerSnapshot(entry.getValue(), timeUnit)) ;
    }
  }
  
  public ClusterMetricRegistrySnapshot(String name) {
    clusterName = name;
  }
  
  public String getClusterName() { return clusterName; }
  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public Map<String, ClusterCounterSnapshot> getCounters() { return counters; }
  public void setCounters(Map<String, ClusterCounterSnapshot> counters) { this.counters = counters; }

  public Map<String, ClusterTimerSnapshot> getTimers() { return timers; }
  public void setTimers(Map<String, ClusterTimerSnapshot> timers) { this.timers = timers; }

  public Map<String, ClusterMetterSnapshot> getMetters() { return metters; }

  public void setMetters(Map<String, ClusterMetterSnapshot> metters) {
    this.metters = metters;
  }
  
  public void add(MetricRegistrySnapshot snapshot) {
    for(Map.Entry<String, CounterSnapshot> sel : snapshot.getCounters().entrySet()) {
      String name = sel.getKey() ;
      ClusterCounterSnapshot clusterCounter = counters.get(name) ;
      if(clusterCounter == null) {
        clusterCounter = new ClusterCounterSnapshot(name) ;
        counters.put(name, clusterCounter);
      }
      clusterCounter.add(sel.getValue());
    }
    
    for(Map.Entry<String, TimerSnapshot> sel : snapshot.getTimers().entrySet()) {
      String name = sel.getKey() ;
      ClusterTimerSnapshot clusterTimer = timers.get(name) ;
      if(clusterTimer == null) {
        clusterTimer = new ClusterTimerSnapshot(name) ;
        timers.put(name, clusterTimer);
      }
      clusterTimer.add(sel.getValue());
    }
    
    for(Map.Entry<String, MetterSnapshot> sel : snapshot.getMetters().entrySet()) {
      String name = sel.getKey() ;
      ClusterMetterSnapshot clusterMetter = metters.get(name) ;
      if(clusterMetter == null) {
        clusterMetter = new ClusterMetterSnapshot(name) ;
        metters.put(name, clusterMetter);
      }
      clusterMetter.add(sel.getValue());
    }
  }
  
  public String getFormattedReport() {
    StringBuilder b = new StringBuilder();
    b.append(getFormattedCounterReport()).append("\n\n");
    b.append(getFormattedTimerReport()).append("\n\n");
    b.append(getFormattedMetterReport()).append("\n\n");
    return b.toString() ;
  }
  
  public String getFormattedCounterReport() {
    String[] header = {"Name", "Count"} ;
    TabularFormater ft = new TabularFormater(header) ;
    ft.setTitle("Counter Report");
    for(ClusterCounterSnapshot sel : counters.values()) {
      ft.addRow(sel.getName(), "");
      for(Map.Entry<String, Long> entry : sel.getCounters().entrySet()) {
        ft.addRow("  " + entry.getKey(), entry.getValue());
      }
      ft.addRow("  Total", sel.getCount());
    }
    return ft.getFormattedText() ;
  }
  
  public String getFormattedTimerReport() {
    String[] header = {
        "Name", "Count",
        "Min", "Max", "Mean", "Std Dev",
        "75%", "90%", "95%", "99%", "99.999",
        "1 Min", "5 Min", "15 Min", "M Rate"
     } ;
    DecimalFormat dFormater = new DecimalFormat("#");
    TabularFormater ft = new TabularFormater(header) ;
    ft.setTitle("Timer Report");
    for(ClusterTimerSnapshot sel : timers.values()) {
      ft.addRow(sel.getName(), "", "", "", "", "", "", "", "", "", "", "", "", "", "");
      for(Map.Entry<String, TimerSnapshot> entry : sel.getTimers().entrySet()) {
        TimerSnapshot timer = entry.getValue();
        ft.addRow(
            "  " + timer.getServerName(), 
            timer.getCount(),
            timer.getMin(),
            timer.getMax(),
            timer.getMean(),
            timer.getStddev(),
            timer.getP75(),
            timer.getP90(),
            timer.getP95(),
            timer.getP99(),
            timer.getP999(),
            dFormater.format(timer.getM1Rate()),
            dFormater.format(timer.getM5Rate()),
            dFormater.format(timer.getM15Rate()),
            dFormater.format(timer.getMeanRate())
        );
      }
    }
    return ft.getFormattedText() ;
  }
  
  public String getFormattedMetterReport() {
    String[] header = {
      "Name", "Count", "1 Min", "5 Min", "15 Min", "M Rate"
    } ;
    DecimalFormat dFormater = new DecimalFormat("#");
    TabularFormater ft = new TabularFormater(header) ;
    ft.setTitle("Metter Report");
    for(ClusterMetterSnapshot sel : metters.values()) {
      ft.addRow(sel.getName(), "", "", "", "", "");
      long count = 0;
      double m1 = 0, m5 = 0, m15 = 0, mean = 0;
      for(Map.Entry<String, MetterSnapshot> entry : sel.getMetters().entrySet()) {
        MetterSnapshot metter = entry.getValue();
        ft.addRow(
            "  " + metter.getServerName(),
            metter.getCount(),
            dFormater.format(metter.getM1Rate()),
            dFormater.format(metter.getM5Rate()),
            dFormater.format(metter.getM15Rate()),
            dFormater.format(metter.getMeanRate())
        );
        count += metter.getCount();
        m1    += metter.getM1Rate();
        m5    += metter.getM5Rate();
        m15   += metter.getM15Rate();
        mean  += metter.getMeanRate();
      }
      ft.addRow(
        "  Total", count, dFormater.format(m1), dFormater.format(m5), dFormater.format(m15), dFormater.format(mean)
      );
    }
    return ft.getFormattedText() ;
  }
}
