package com.neverwinterdp.analytics.web;

import com.neverwinterdp.netty.http.client.ClientInfo;

public class WebEvent {
  private String eventId;
  private long   timestamp;
  private String name;

  private ClientInfo  clientInfo;
  
  public WebEvent() { }

  public String getEventId() { return eventId; }
  public void setEventId(String id) { this.eventId = id; }

  public long getTimestamp() { return timestamp; }
  public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

  public String getName() { return name; }
  public void setName(String name) { this.name = name; }

  public ClientInfo getClientInfo() { return clientInfo; }
  public void setClientInfo(ClientInfo info) { this.clientInfo = info; }
}