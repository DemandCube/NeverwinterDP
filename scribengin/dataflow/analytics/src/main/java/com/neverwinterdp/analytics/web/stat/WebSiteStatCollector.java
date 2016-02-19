package com.neverwinterdp.analytics.web.stat;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.neverwinterdp.analytics.web.WebEvent;
import com.neverwinterdp.util.UrlParser;

public class WebSiteStatCollector {
  private Map<String, PeriodWebSiteStatCollector> holder = new HashMap<String, PeriodWebSiteStatCollector>();
  
  public WebSiteStatCollector() {
  }
  
  public List<WebSiteStat> takeWebSiteStat() {
    List<WebSiteStat> wssHolder = new ArrayList<>();
    for(PeriodWebSiteStatCollector sel : holder.values()) {
      wssHolder.addAll(sel.takeWebSiteStat());
    }
    holder.clear();
    return wssHolder;
  }
  
  public void log(long periodTimestamp, UrlParser urlParser, WebEvent webEvent) {
    PeriodWebSiteStatCollector select = holder.get(urlParser.getUrl());
    if(select == null) {
      select = new PeriodWebSiteStatCollector(urlParser.getUrl());
      holder.put(select.host, select);
    }
    select.log(periodTimestamp, urlParser, webEvent);
  }
  
  static public class PeriodWebSiteStatCollector {
    private String                 host;
    private Map<Long, WebSiteStat> holder = new HashMap<Long, WebSiteStat>();
    
    public PeriodWebSiteStatCollector(String host) {
      this.host = host;
    }
    
    public String getHost() { return host; }
    
    public List<WebSiteStat> takeWebSiteStat() {
      List<WebSiteStat> wssHolder = new ArrayList<>();
      wssHolder.addAll(holder.values());
      holder.clear();
      return wssHolder;
    }
    
    public void log(long periodTimestamp, UrlParser urlParser, WebEvent webEvent) {
      WebSiteStat wsStat = holder.get(periodTimestamp);
      if(wsStat == null) {
        wsStat = new WebSiteStat(new Date(periodTimestamp), urlParser);
        holder.put(periodTimestamp, wsStat);
      }
      wsStat.log(periodTimestamp, urlParser, webEvent);
    }
  }
}