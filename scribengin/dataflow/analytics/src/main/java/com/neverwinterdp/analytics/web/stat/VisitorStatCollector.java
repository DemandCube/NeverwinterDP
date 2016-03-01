package com.neverwinterdp.analytics.web.stat;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.neverwinterdp.analytics.web.WebEvent;

public class VisitorStatCollector {
  private Map<Long, ByPeriodVisitorStatCollector> mapHolder = new HashMap<>();
  
  synchronized public List<VisitorStat> takeVisitorStats() {
    List<VisitorStat> holder = new ArrayList<>() ;
    for(ByPeriodVisitorStatCollector sel : mapHolder.values()) {
      holder.addAll(sel.takeVisitorStats());
    }
    mapHolder.clear();
    return holder;
  }
  
  public void log(long periodTimestamp, String host, WebEvent webEvent) {
    ByPeriodVisitorStatCollector periodCollector = mapHolder.get(periodTimestamp);
    if(periodCollector == null) {
      periodCollector = new ByPeriodVisitorStatCollector(periodTimestamp);
      mapHolder.put(periodTimestamp, periodCollector);
    }
    periodCollector.log(periodTimestamp, host, webEvent);
  }
  
  static public class ByPeriodVisitorStatCollector {
    private long periodTimestamp ;
    private Map<String, VisitorStat> mapHolder = new HashMap<>();
    
    public ByPeriodVisitorStatCollector(long periodTimestamp) {
      this.periodTimestamp = periodTimestamp; 
    }
    
    synchronized public List<VisitorStat> takeVisitorStats() {
      List<VisitorStat> holder = new ArrayList<>(mapHolder.values()) ;
      mapHolder.clear();
      return holder;
    }
    
    synchronized public void log(long periodTimestamp, String host, WebEvent webEvent) {
      String visitorId = webEvent.getClientInfo().user.visitorId;
      String key = host + ":" + visitorId;
      VisitorStat stat = mapHolder.get(key);
      if(stat == null) {
        stat = new  VisitorStat(new Date(periodTimestamp), host, visitorId);
        mapHolder.put(key, stat);
      }
      stat.log(periodTimestamp, host, visitorId, webEvent);
    }
  }
  
}