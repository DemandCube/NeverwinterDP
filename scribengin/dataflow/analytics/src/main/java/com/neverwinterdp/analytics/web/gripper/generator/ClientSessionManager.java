package com.neverwinterdp.analytics.web.gripper.generator;

import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.netty.http.client.ClientInfo;

public class ClientSessionManager {
  @Parameter(names = "--num-of-users", description = "num of users")
  private int numOfUsers = 5;
  
  @Parameter(names = "--num-of-sites", description = "num of users")
  private int numOfSites = 10;

  @Parameter(names = "--min-page-visit-per-site", description = "num of users")
  private int minPageVisitPerSite = 5 ;
  
  @Parameter(names = "--max-page-visit-per-site", description = "num of users")
  private int maxPageVisitPerSite = 30;
  
  @Parameter(names = "--num-of-pages", description = "num of pages")
  private int numOfPages = 10000;
  
  @Parameter(names = "--max-visit-time", description = "max visit time")
  private int maxVisitTime = 0;
  
  @Parameter(names = "--min-visit-time", description = "min visit time")
  private int minVisitTime = 0;
  
  private String[]             availUsers;
  private String[]             availSites;
  private AtomicInteger        sessionIdTracker = new AtomicInteger();
  private Random               random           = new Random(System.nanoTime());
  private int                  numOfAssignedPages;
  
  private LinkedBlockingQueue<ClientSession> queue = new LinkedBlockingQueue<>();
  private int  maxConcurrentClientSession = 1000;
  
  public int getNumOfUsers() { return numOfUsers; }
  public void setNumOfUsers(int numOfUsers) { this.numOfUsers = numOfUsers; }

  public int getNumOfSites() { return numOfSites; }
  public void setNumOfSites(int numOfSites) { this.numOfSites = numOfSites; }

  public int getMinPageVisitPerSite() { return minPageVisitPerSite; }
  public void setMinPageVisitPerSite(int minPageVisitPerSite) {
    this.minPageVisitPerSite = minPageVisitPerSite;
  }

  public int getMaxPageVisitPerSite() { return maxPageVisitPerSite; }
  public void setMaxPageVisitPerSite(int maxPageVisitPerSite) {
    this.maxPageVisitPerSite = maxPageVisitPerSite;
  }

  public int getNumOfPages() { return numOfPages; }
  public void setNumOfPages(int numOfPages) { this.numOfPages = numOfPages; }

  public void start() {
    availUsers    = new String[numOfUsers];
    for(int i = 0; i < availUsers.length; i++) {
      availUsers[i] = "user-" + (i + 1);
    }
    
    availSites    = new String[numOfSites];
    for(int i = 0; i < availSites.length; i++) {
      availSites[i] = "www.website-" + (i + 1) + ".com";
    }
    
    numOfAssignedPages = 0;
  }
  
  public ClientSession pollClientSession() throws InterruptedException {
    if(queue.size() < maxConcurrentClientSession) {
      ClientSession session = createClientSession();
      if(session != null) queue.offer(session);
    }
    return queue.poll(1000, TimeUnit.MILLISECONDS);
  }
  
  public void offerClientSession(ClientSession session) throws InterruptedException {
    queue.offer(session);
  }
  
  ClientSession createClientSession() {
    if(numOfAssignedPages >= numOfPages) return null;
    String user =  nextRandomUser();
    String sessionId = "generated-" + user + "-session-" + sessionIdTracker.incrementAndGet(); 
    ClientInfo clientInfo = ClientInfos.nextRandomClientInfo();
    clientInfo.user.userId = user;
    clientInfo.user.visitorId = sessionId;
    ClientSession session = new ClientSession(user, sessionId, clientInfo, maxVisitTime, minVisitTime);
    int numOfSitePerSession = random.nextInt(numOfSites) + 1;
    for(int i = 0; i < numOfSitePerSession; i++) {
      String selSite = nextRandomSite();
      int numOfPagePerSite = random.nextInt(maxPageVisitPerSite);
      if(numOfPagePerSite < minPageVisitPerSite) {
        numOfPagePerSite = minPageVisitPerSite;
      }
      if((numOfAssignedPages + numOfPagePerSite) > numOfPages) {
        numOfPagePerSite = numOfPages - numOfAssignedPages;
      }
      numOfAssignedPages += numOfPagePerSite;
      session.addSiteVisit(selSite, numOfPagePerSite);
      
      if(numOfAssignedPages == numOfPages) break;
      else if(numOfAssignedPages > numOfPages) {
        throw new RuntimeException("This should not happen");
      }
    }
    session.init();
    return session;
  }
  
  String nextRandomUser() {
    int sel = random.nextInt(availUsers.length);
    return availUsers[sel];
  }
  
  String nextRandomSite() {
    int sel = random.nextInt(availSites.length);
    return availSites[sel];
  }
}
