package com.neverwinterdp.yara.snapshot;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.neverwinterdp.yara.Counter;
import com.neverwinterdp.yara.cluster.ClusterCounter;

public class ClusterCounterSnapshot implements Serializable {
  private String name ;
  private long   count ;
  private Map<String, Long> counters = new HashMap<String, Long>() ;

  public ClusterCounterSnapshot() { }
  
  public ClusterCounterSnapshot(String key, ClusterCounter clusterCounter) {
    this.name = key ;
    this.count = clusterCounter.getCounter().getCount() ;
    for(Map.Entry<String, Counter> entry : clusterCounter.getCounters().entrySet()) {
      counters.put(entry.getKey(), entry.getValue().getCount()) ;
    }
  }

  public ClusterCounterSnapshot(String name) { 
    this.name = name ;
  }
  
  public String getName() { return name; }
  public void   setName(String key) { this.name = key; }

  public long getCount() { return count; }
  public void setCount(long count) { this.count = count; }

  public Map<String, Long> getCounters() { return counters; }
  public void setCounters(Map<String, Long> counters) { this.counters = counters;}

  public void add(CounterSnapshot snapshot) {
    if(!name.equals(snapshot.getName())) {
      throw new RuntimeException("expect name " + name) ;
    }
    count += snapshot.getCount();
    counters.put(snapshot.getServerName(), snapshot.getCount());
  }
}