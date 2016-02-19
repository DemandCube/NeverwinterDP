package com.neverwinterdp.analytics.web.generator;

import java.net.ConnectException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.neverwinterdp.analytics.web.BrowserInfo;
import com.neverwinterdp.analytics.web.WebEvent;
import com.neverwinterdp.netty.http.client.AsyncHttpClient;

public class BrowserSession {
  private String        username;
  private String        sessionId;
  private AtomicInteger idTracker = new AtomicInteger();
  
  private BrowserInfo     browserInfo;
  private List<SiteVisit> siteToVisits ;
  
  public BrowserSession(String username, String sessionId, BrowserInfo bInfo) {
    this.username     = username;
    this.sessionId    = sessionId;
    this.browserInfo  = bInfo;
    this.siteToVisits = new ArrayList<>();
  }
  
  public void addSiteVisit(String site, int numOfPages) {
    siteToVisits.add(new SiteVisit(site, numOfPages));
  }
  
  public int countNumPages() {
    int total = 0;
    for(int i = 0; i < siteToVisits.size(); i++) {
      total = siteToVisits.size();
    }
    return total;
  }
  
  public int visit(AsyncHttpClient client) throws ConnectException, URISyntaxException, InterruptedException {
    Collections.shuffle(siteToVisits, new Random(System.nanoTime()));
    List<String> pages = new ArrayList<>();
    for(int i = 0; i < siteToVisits.size(); i++) {
      SiteVisit sel = siteToVisits.get(i);
      pages.addAll(sel.getPages());
    }
    
    for(int i = 0; i < pages.size(); i++) {
      WebEvent wEvent = new WebEvent();
      wEvent.setUsername(username);
      wEvent.setSessionId(sessionId);
      wEvent.setEventId(sessionId + "-" + idTracker.incrementAndGet());
      wEvent.setTimestamp(System.currentTimeMillis());
      wEvent.setName("user-click");
      wEvent.setMethod("GET");
      wEvent.setUrl(pages.get(i));
      wEvent.setBrowserInfo(browserInfo);
      client.post("/webevent/user.click", wEvent);
    }
    return pages.size();
  }
}
