package com.neverwinterdp.analytics.ads;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;

public class ADSEvent {
  private String eventId;
  
  @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd/MM/yyyy HH:mm:ss")
  private Date timestamp;
  
  private String name ;
  private String visitId;
  
  private String host;
  private String webpageUrl;
  private String webpageReferralUrl;
  private String adUrl;
  private String visitorId;
  private String clientIpAddress;
  
  public String getEventId() { return eventId; }
  public void   setEventId(String eventId) { this.eventId = eventId; }
  
  public Date getTimestamp() { return timestamp; }
  public void setTimestamp(Date timestamp) { this.timestamp = timestamp; }
  
  public String getName() { return name; }
  public void setName(String name) { this.name = name; }
  
  public String getVisitId() { return visitId; }
  public void setVisitId(String visitId) { this.visitId = visitId; }
  
  public String getHost() { return host; }
  public void setHost(String host) { this.host = host; }

  public String getWebpageUrl() { return webpageUrl; }
  public void setWebpageUrl(String webpageUrl) { this.webpageUrl = webpageUrl; }
  
  public String getWebpageReferralUrl() { return webpageReferralUrl; }
  public void setWebpageReferralUrl(String webpageReferralUrl) { this.webpageReferralUrl = webpageReferralUrl; }
  
  public String getAdUrl() { return adUrl; }
  public void setAdUrl(String adUrl) { this.adUrl = adUrl; }
  
  public String getVisitorId() { return visitorId; }
  public void setVisitorId(String visitorId) { this.visitorId = visitorId; }
  
  public String getClientIpAddress() { return clientIpAddress; }
  public void setClientIpAddress(String clientIpAddress) { this.clientIpAddress = clientIpAddress; }
}
