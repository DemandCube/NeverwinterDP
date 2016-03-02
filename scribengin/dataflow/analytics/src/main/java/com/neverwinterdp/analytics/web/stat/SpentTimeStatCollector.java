package com.neverwinterdp.analytics.web.stat;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.neverwinterdp.analytics.web.WebEvent;

public class SpentTimeStatCollector {
  private Map<Long, ByPeriodSpentTimeStatCollector> mapHolder = new HashMap<>();
  
  synchronized public List<SpentTimeStat> takeSpentTimeStats() {
    List<SpentTimeStat> holder = new ArrayList<>() ;
    for(ByPeriodSpentTimeStatCollector sel : mapHolder.values()) {
      holder.addAll(sel.takeSpentTimeStats());
    }
    mapHolder.clear();
    return holder;
  }
  
  public void log(long periodTimestamp, String host, WebEvent webEvent) {
    ByPeriodSpentTimeStatCollector periodCollector = mapHolder.get(periodTimestamp);
    if(periodCollector == null) {
      periodCollector = new ByPeriodSpentTimeStatCollector(periodTimestamp);
      mapHolder.put(periodTimestamp, periodCollector);
    }
    periodCollector.log(periodTimestamp, host, webEvent);
  }
  
  static public class ByPeriodSpentTimeStatCollector {
    private long periodTimestamp ;
    private Map<String, SpentTimeStat> mapHolder = new HashMap<>();
    
    public ByPeriodSpentTimeStatCollector(long periodTimestamp) {
      this.periodTimestamp = periodTimestamp; 
    }
    
    synchronized public List<SpentTimeStat> takeSpentTimeStats() {
      List<SpentTimeStat> holder = new ArrayList<>(mapHolder.values()) ;
      mapHolder.clear();
      return holder;
    }
    
    synchronized public void log(long periodTimestamp, String host, WebEvent webEvent) {
      SpentTimeStat stat = mapHolder.get(host);
      if(stat == null) {
        stat = new  SpentTimeStat(new Date(periodTimestamp), host);
        mapHolder.put(host, stat);
      }
      stat.log(periodTimestamp, host, webEvent);
    }
  }
  
}