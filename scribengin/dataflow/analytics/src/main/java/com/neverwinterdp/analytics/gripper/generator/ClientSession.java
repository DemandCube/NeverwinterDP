package com.neverwinterdp.analytics.gripper.generator;

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

public class ClientSession {
  private String        username;
  private String        sessionId;
  private AtomicInteger idTracker = new AtomicInteger();

  private ClientInfo      clientInfo;
  private List<SiteVisit> siteToVisits;
  private List<String>    pagesToVisit;
  private int             currentVisitPage = 0;
  private int             maxVisitTime = 0;
  private int             minVisitTime = 0;
  private Random          rand         = new Random();

  public ClientSession(String username, String sessionId, ClientInfo cInfo, int maxVisitTime, int minVisitTime) {
    this.username     = username;
    this.sessionId    = sessionId;
    this.clientInfo   = cInfo;
    this.maxVisitTime = maxVisitTime;
    this.minVisitTime = minVisitTime;
    this.siteToVisits = new ArrayList<>();
  }
  
  public void init() {
    Collections.shuffle(siteToVisits, new Random(System.nanoTime()));
    pagesToVisit = new ArrayList<>();
    for(int i = 0; i < siteToVisits.size(); i++) {
      SiteVisit sel = siteToVisits.get(i);
      pagesToVisit.addAll(sel.getPages());
    }
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
  
  public boolean hasNextWebEvent() { return currentVisitPage < pagesToVisit.size(); }
  
  public void sendWebEvent(AsyncHttpClient client, String dest) throws ConnectException, URISyntaxException, InterruptedException {
    clientInfo.webpage.url = pagesToVisit.get(currentVisitPage++);
    clientInfo.webpage.startVisitTime = System.currentTimeMillis();
    clientInfo.webpage.endVisitTime = clientInfo.webpage.startVisitTime + rand.nextInt(maxVisitTime);
    client.post(dest, clientInfo);
    if(rand.nextDouble() < 0.3) {
      client.post(dest, clientInfo);
    }
  }
  
  public void sendADSEvent(AsyncHttpClient client, String dest) throws ConnectException, URISyntaxException, InterruptedException {
    ADSEvent event = new ADSEvent();
    event.setSource("generator");
    event.setVisitorId(clientInfo.user.visitorId);
    event.setName("BotNventdata");
    event.setAdUrl("http://nventdata.com");
    event.setWebpageUrl(clientInfo.webpage.url);
    client.post(dest, event);
    //System.out.println("Generate ADSEvent: " + JSONSerializer.INSTANCE.toString(event));
  }
}
