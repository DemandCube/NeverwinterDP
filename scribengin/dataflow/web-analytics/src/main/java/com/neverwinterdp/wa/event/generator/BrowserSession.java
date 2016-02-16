package com.neverwinterdp.wa.event.generator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.neverwinterdp.netty.http.client.AsyncHttpClient;
import com.neverwinterdp.wa.event.BrowserInfo;
import com.neverwinterdp.wa.event.WebEvent;

public class BrowserSession {
  private String        sessionId;
  private AtomicInteger idTracker = new AtomicInteger();
  
  private BrowserInfo     browserInfo;
  private List<SiteVisit> siteToVisits ;
  
  public BrowserSession(BrowserInfo bInfo) {
    browserInfo  = bInfo;
    siteToVisits = new ArrayList<>();
  }
  
  public void addSiteVisit(String site, int numOfPages) {
    siteToVisits.add(new SiteVisit(site, numOfPages));
  }
  
  public void visit(AsyncHttpClient client) {
    long seed = System.nanoTime();
    Collections.shuffle(siteToVisits, new Random(seed));
    List<String> pages = new ArrayList<>();
    for(int i = 0; i < siteToVisits.size(); i++) {
      SiteVisit sel = siteToVisits.get(i);
      pages.addAll(sel.getPages());
    }
    
    for(int i = 0; i < pages.size(); i++) {
      WebEvent event = new WebEvent();
      event.setId(Integer.toString(idTracker.incrementAndGet()));
      event.setTimestamp(System.currentTimeMillis());
      event.setName("user-click");
      event.setMethod("GET");
      event.setUrl(pages.get(i));
      event.setBrowserInfo(browserInfo);
    }
  }
}
