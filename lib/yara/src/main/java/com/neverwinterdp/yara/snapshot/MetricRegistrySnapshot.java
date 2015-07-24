package com.neverwinterdp.yara.snapshot;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import com.neverwinterdp.util.text.TabularFormater;
import com.neverwinterdp.yara.Counter;
import com.neverwinterdp.yara.Meter;
import com.neverwinterdp.yara.MetricRegistry;
import com.neverwinterdp.yara.Timer;

public class MetricRegistrySnapshot implements Serializable {
  private String serverName ;
  private TreeMap<String, CounterSnapshot> counters = new TreeMap<>() ;
  private TreeMap<String, TimerSnapshot>   timers   = new TreeMap<>() ;
  private TreeMap<String, MetterSnapshot>   metters   = new TreeMap<>() ;

  public MetricRegistrySnapshot() {}
  
  public MetricRegistrySnapshot(String serverName, MetricRegistry registry) {
    this(serverName, registry, TimeUnit.MILLISECONDS) ;
  }
  
  public MetricRegistrySnapshot(String serverName, MetricRegistry registry, TimeUnit timeUnit) {
    this.serverName = serverName;
    for(Map.Entry<String, Counter> entry : registry.getCounters().entrySet()) {
      counters.put(entry.getKey(), new CounterSnapshot(serverName, entry.getValue())) ;
    }
    
    for(Map.Entry<String, Timer> entry : registry.getTimers().entrySet()) {
      timers.put(entry.getKey(), new TimerSnapshot(serverName, entry.getValue(), timeUnit)) ;
    }
    
    for(Map.Entry<String, Meter> entry : registry.getMeters().entrySet()) {
      metters.put(entry.getKey(), new MetterSnapshot(serverName, entry.getValue())) ;
    }
  }
  
  public String getServerName() { return serverName; }
  public void setServerName(String serverName) { this.serverName = serverName; }

  public TreeMap<String, CounterSnapshot> getCounters() { return counters; }
  public void setCounters(TreeMap<String, CounterSnapshot> counters) { this.counters = counters; }
  
  public TreeMap<String, TimerSnapshot> getTimers() { return timers; }
  public void setTimers(TreeMap<String, TimerSnapshot> timers) { this.timers = timers; }

  public TreeMap<String, MetterSnapshot> getMetters() { return metters; }
  public void setMetters(TreeMap<String, MetterSnapshot> metters)  { this.metters = metters; }
  
  static public String getFormattedText(List<MetricRegistrySnapshot> snapshots) {
    return new MetricRegistrySnapshotFormater(snapshots).getFormattedText();
  }
  
  static public class MetricRegistrySnapshotFormater {
    private TreeSet<String> counterKeys = new TreeSet<String>();
    private TreeSet<String> timerKeys   = new TreeSet<String>();
    private TreeSet<String> metricKeys  = new TreeSet<String>();
    private List<MetricRegistrySnapshot> snapshots ;
    
    public MetricRegistrySnapshotFormater(List<MetricRegistrySnapshot> list) {
      snapshots = list ;
      for(int i = 0; i < list.size(); i++) {
        MetricRegistrySnapshot snapshot = list.get(i);
        counterKeys.addAll(snapshot.getCounters().keySet()) ;
        timerKeys.addAll(snapshot.getTimers().keySet()) ;
        metricKeys.addAll(snapshot.getMetters().keySet()) ;
      }
    }
    
    public String getFormattedText() {
      StringBuilder b = new StringBuilder() ;
      b.append(getFormattedCounter()).append("\n\n");
      b.append(getFormattedTimer()).append("\n\n");
      b.append(getFormattedMeter());
      return b.toString();
    }
    
    public String getFormattedCounter() {
      TabularFormater counterFt = new TabularFormater("Name", "Count") ;
      counterFt.setTitle("Counter");
      for(String key : counterKeys) {
        counterFt.addRow(key, "");
        long total = 0 ;
        for(MetricRegistrySnapshot sel : snapshots) {
          CounterSnapshot counter = sel.getCounters().get(key);
          if(counter != null) {
            counterFt.addRow(" - " + counter.getServerName(), counter.getCount());
            total += counter.getCount();
          }
          counterFt.addRow(" - Total ", total);
        }
      }
      return counterFt.getFormattedText();
    }
    
    public String getFormattedTimer() {
      DecimalFormat dFormater = new DecimalFormat("#");
      TabularFormater timerFt = new TabularFormater(
          "Name", "Count",
          "Min", "Max", "Mean", "Std Dev",
          "75%", "90%", "95%", "99%", "99.999",
          "1 Min", "5 Min", "15 Min", "M Rate"
      ) ;
      timerFt.setTitle("Timer");
      for(String key : timerKeys) {
        timerFt.addRow(key, "", "", "",  "", "", "",  "", "", "",  "", "", "", "", "");
        for(MetricRegistrySnapshot sel : snapshots) {
          TimerSnapshot timer = sel.getTimers().get(key);
          if(timer != null) {
            timerFt.addRow(
              " - " + timer.getServerName(), 
              timer.getCount(),
              dFormater.format(timer.getMin()), 
              dFormater.format(timer.getMax()), 
              dFormater.format(timer.getMean()), 
              dFormater.format(timer.getStddev()), 
              
              dFormater.format(timer.getP75()),
              dFormater.format(timer.getP90()),
              dFormater.format(timer.getP95()),
              dFormater.format(timer.getP99()),
              dFormater.format(timer.getP999()),
              
              dFormater.format(timer.getM1Rate()), 
              dFormater.format(timer.getM5Rate()), 
              dFormater.format(timer.getM15Rate()),
              dFormater.format(timer.getMeanRate())
            );
          }
        }
      }
      return timerFt.getFormattedText();
    }
    
    public String getFormattedMeter() {
      DecimalFormat dFormater = new DecimalFormat("#");
      TabularFormater meterFt = new TabularFormater(
          "Name", "Count",
          "1 Min", "5 Min", "15 Min", "M Rate"
      ) ;
      meterFt.setTitle("Meter");
      for(String key : metricKeys) {
        meterFt.addRow(key, "", "", "", "", "");
        for(MetricRegistrySnapshot sel : snapshots) {
          MetterSnapshot meter = sel.getMetters().get(key);
          if(meter != null) {
            meterFt.addRow(
              " - " + meter.getServerName(), 
              meter.getCount(),
              dFormater.format(meter.getM1Rate()), 
              dFormater.format(meter.getM5Rate()), 
              dFormater.format(meter.getM15Rate()),
              dFormater.format(meter.getMeanRate())
            );
          }
        }
      }
      return meterFt.getFormattedText();
    }
  }
}