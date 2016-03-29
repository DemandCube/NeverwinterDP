package com.neverwinterdp.analytics.web;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;

public class WebpageVisit {
  private String source  = "user";
  private String eventId;
  
  @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd/MM/yyyy HH:mm:ss")
  private Date timestamp;
  
  private String visitId;
  
  private String host;
  private String path;
  
  private String visitorId;
  private String visitorRegion;
  private double visitorLatitude;
  private double visitorLongitude;
  
  private String clientIpAddress;
  private long   spentTime;
  private String spentTimeRange ;

  public WebpageVisit() {} 
  
  public String getSource() { return source; }
  public void setSource(String source) { this.source = source; }

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
  
  public String getVisitorRegion() { return visitorRegion; }
  public void setVisitorRegion(String visitorRegion) { this.visitorRegion = visitorRegion; }

  public double getVisitorLatitude() { return visitorLatitude; }
  public void setVisitorLatitude(double visitorLatitude) { this.visitorLatitude = visitorLatitude; }
  
  public double getVisitorLongitude() { return visitorLongitude; }
  public void setVisitorLongitude(double visitorLongitude) { this.visitorLongitude = visitorLongitude; }

  public String getClientIpAddress() { return clientIpAddress; }
  public void setClientIpAddress(String clientIpAddress) { this.clientIpAddress = clientIpAddress; }

  public long getSpentTime() { return spentTime; }
  public void setSpentTime(long spentTime) { this.spentTime = spentTime; }

  public String getSpentTimeRange() { return spentTimeRange; }
  public void setSpentTimeRange(String spentTimeRange) { this.spentTimeRange = spentTimeRange; }

  public void logSpentTime(long spentTime) {
    this.spentTime = spentTime;
    if(spentTime <         1000) spentTimeRange = "1s";
    else if(spentTime <    2000) spentTimeRange = "2s";
    else if(spentTime <    3000) spentTimeRange = "3s";
    else if(spentTime <    5000) spentTimeRange = "5s";
    else if(spentTime <   10000) spentTimeRange = "10s";
    else if(spentTime <   30000) spentTimeRange = "30s";
    else if(spentTime <   60000) spentTimeRange = "60s";
    else if(spentTime <  180000) spentTimeRange = "180s";
    else                         spentTimeRange = "300s";
  }
}
