package com.neverwinterdp.analytics.web.gripper.generator;

import java.net.ConnectException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.neverwinterdp.analytics.ads.ADSEvent;
import com.neverwinterdp.netty.http.client.AsyncHttpClient;
import com.neverwinterdp.netty.http.client.ClientInfo;
import com.neverwinterdp.util.JSONSerializer;

public class BrowserSession {
  private String        username;
  private String        sessionId;
  private AtomicInteger idTracker = new AtomicInteger();
  
  private ClientInfo      clientInfo;
  private List<SiteVisit> siteToVisits ;
  private int maxVisitTime = 0;
  private int minVisitTime = 0;
  private Random rand      = new Random();

  public BrowserSession(String username, String sessionId, ClientInfo cInfo, int maxVisitTime, int minVisitTime) {
    this.username     = username;
    this.sessionId    = sessionId;
    this.clientInfo   = cInfo;
    this.maxVisitTime = maxVisitTime;
    this.minVisitTime = minVisitTime;
    this.siteToVisits = new ArrayList<>();
  }
  
  public String getSessionId() { return this.sessionId ; }
  
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
      clientInfo.webpage.url = pages.get(i);
      client.post(dest, clientInfo);
      if(maxVisitTime > 0) {
        int visitTime = rand.nextInt(maxVisitTime);
        if(visitTime < minVisitTime) visitTime = minVisitTime;
        Thread.sleep(visitTime);
      }
    }
    return pages.size();
  }
  
  public void sendADSEvent(AsyncHttpClient client, String dest) throws ConnectException, URISyntaxException, InterruptedException {
    ADSEvent event = new ADSEvent();
    event.setVisitorId(clientInfo.user.visitorId);
    event.setAdUrl("http://nventdata.com");
    event.setWebpageUrl(clientInfo.webpage.url);
    client.post(dest, event);
    System.err.println("Generate ADSEvent: " + JSONSerializer.INSTANCE.toString(event));
  }
}
