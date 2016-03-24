package com.neverwinterdp.analytics.web;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;

public class WebpageVisit {
  private String eventId;
  
  @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd/MM/yyyy HH:mm:ss")
  private Date timestamp;
  
  private String visitId;
  
  private String host;
  private String path;
  private String visitorId;
  private String clientIpAddress;
  private long   spentTime;

  public WebpageVisit() {} 
  
  public String getEventId() { return eventId; }
  public void   setEventId(String eventId) { this.eventId = eventId; }
  
  public Date getTimestamp() { return timestamp; }
  public void setTimestamp(Date timestamp) { this.timestamp = timestamp; }
  
  public String getVisitId() { return visitId; }
  public void setVisitId(String visitId) { this.visitId = visitId; }
  
  public String getHost() { return host; }
  public void setHost(String host) { this.host = host; }

  public String getPath() { return path; }
  public void setPath(String path) { this.path = path; }
  
  public String getVisitorId() { return visitorId; }
  public void setVisitorId(String visitorId) { this.visitorId = visitorId; }
  
  public String getClientIpAddress() { return clientIpAddress; }
  public void setClientIpAddress(String clientIpAddress) { this.clientIpAddress = clientIpAddress; }

  public long getSpentTime() { return spentTime; }
  public void setSpentTime(long spentTime) { this.spentTime = spentTime; }
}
