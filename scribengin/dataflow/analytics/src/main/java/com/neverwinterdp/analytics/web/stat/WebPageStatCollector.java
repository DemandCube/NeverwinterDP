package com.neverwinterdp.analytics.web.stat;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.neverwinterdp.analytics.web.WebEvent;
import com.neverwinterdp.util.UrlParser;

public class WebPageStatCollector {
  private Map<String, PeriodWebPageStatCollector> holder = new HashMap<String, PeriodWebPageStatCollector>();
  
  public WebPageStatCollector() {
  }
  
  public List<WebPageStat> takeWebPageStat() {
    List<WebPageStat> wpsHolder = new ArrayList<>();
    for(PeriodWebPageStatCollector sel : holder.values()) {
      wpsHolder.addAll(sel.takeWebPageStat());
    }
    holder.clear();
    return wpsHolder;
  }
  
  public void log(long periodTimestamp, UrlParser urlParser, WebEvent webEvent) {
    String url = urlParser.getUrl();
    PeriodWebPageStatCollector select = holder.get(url);
    if(select == null) {
      select = new PeriodWebPageStatCollector(url);
      holder.put(url, select);
    }
    select.log(periodTimestamp, urlParser, webEvent);
  }
  
  static public class PeriodWebPageStatCollector {
    private String                 pageUrl;
    private Map<Long, WebPageStat> holder = new HashMap<Long, WebPageStat>();
    
    public PeriodWebPageStatCollector(String pageUrl) {
      this.pageUrl = pageUrl;
    }
    
    public String getPageUrl() { return pageUrl; }
    
    public List<WebPageStat> takeWebPageStat() {
      List<WebPageStat> wpsHolder = new ArrayList<>();
      wpsHolder.addAll(holder.values());
      holder.clear();
      return wpsHolder;
    }
    
    public void log(long periodTimestamp, UrlParser urlParser, WebEvent webEvent) {
      Long key = new Long(periodTimestamp);
      WebPageStat wpStat = holder.get(key);
      if(wpStat == null) {
        wpStat = new WebPageStat(new Date(periodTimestamp), urlParser);
        holder.put(key, wpStat);
      }
      wpStat.log(periodTimestamp, urlParser, webEvent);
    }
  }
}