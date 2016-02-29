package com.neverwinterdp.analytics.web;

import com.neverwinterdp.netty.http.client.ClientInfo;

public class WebEvent {
  private String eventId;
  private long   timestamp;
  private String name;

  private ClientInfo  clientInfo;
  private String      method;
  private String      url ;
  private String      referralUrl;
  
  public WebEvent() { }

  public String getEventId() { return eventId; }
  public void setEventId(String id) { this.eventId = id; }

  public long getTimestamp() { return timestamp; }
  public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

  public String getName() { return name; }
  public void setName(String name) { this.name = name; }

  public ClientInfo getClientInfo() { return clientInfo; }
  public void setClientInfo(ClientInfo info) { this.clientInfo = info; }

  public String getMethod() { return method; }
  public void setMethod(String method) { this.method = method; }

  public String getUrl() { return url; }
  public void setUrl(String url) { this.url = url; }

  public String getReferralUrl() { return referralUrl; }
  public void setReferralUrl(String referralUrl) { this.referralUrl = referralUrl; }
}