package com.neverwinterdp.analytics.web.gripper.generator;

import java.net.ConnectException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.neverwinterdp.analytics.web.WebEvent;
import com.neverwinterdp.netty.http.client.AsyncHttpClient;
import com.neverwinterdp.netty.http.client.ClientInfo;

public class BrowserSession {
  private String        username;
  private String        sessionId;
  private AtomicInteger idTracker = new AtomicInteger();
  
  private ClientInfo      clientInfo;
  private List<SiteVisit> siteToVisits ;
  
  public BrowserSession(String username, String sessionId, ClientInfo cInfo) {
    this.username     = username;
    this.sessionId    = sessionId;
    this.clientInfo   = cInfo;
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
  
  public int sendWebEvent(AsyncHttpClient client, String dest) throws ConnectException, URISyntaxException, InterruptedException {
    Collections.shuffle(siteToVisits, new Random(System.nanoTime()));
    List<String> pages = new ArrayList<>();
    for(int i = 0; i < siteToVisits.size(); i++) {
      SiteVisit sel = siteToVisits.get(i);
      pages.addAll(sel.getPages());
    }
    
    for(int i = 0; i < pages.size(); i++) {
      WebEvent wEvent = new WebEvent();
      wEvent.setEventId(sessionId + "-" + idTracker.incrementAndGet());
      wEvent.setTimestamp(System.currentTimeMillis());
      wEvent.setName("user-click");
      wEvent.getClientInfo().webpage.url = pages.get(i);
      wEvent.setClientInfo(clientInfo);
      client.post("/rest/webevent/" + dest, wEvent);
    }
    return pages.size();
  }
}
