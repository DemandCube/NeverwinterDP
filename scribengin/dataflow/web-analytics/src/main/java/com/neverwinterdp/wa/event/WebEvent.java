package com.neverwinterdp.wa.event;

public class WebEvent {
  private String id;
  private long   timestamp;
  private String name;

  private BrowserInfo browserInfo;
  private String      method;
  private String      url ;
  private String      referralUrl;
  
  public WebEvent() { }

  public String getId() { return id; }

  public void setId(String id) { this.id = id; }

  public long getTimestamp() { return timestamp; }

  public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

  public String getName() { return name; }
  public void setName(String name) { this.name = name; }

  public BrowserInfo getBrowserInfo() { return browserInfo; }
  public void setBrowserInfo(BrowserInfo browserInfo) { this.browserInfo = browserInfo; }

  public String getMethod() { return method; }
  public void setMethod(String method) { this.method = method; }

  public String getUrl() { return url; }
  public void setUrl(String url) { this.url = url; }

  public String getReferralUrl() { return referralUrl; }
  public void setReferralUrl(String referralUrl) { this.referralUrl = referralUrl; }
}
