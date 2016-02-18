package com.neverwinterdp.scribengin.dataflow.sample;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;

public class WebEvent {

  private String name;
  private String webEventId;
  
  @JsonFormat(shape=JsonFormat.Shape.STRING, pattern="dd/MM/yyyy HH:mm:ss")
  private Date   timestamp;
  
  private WebEventAttributes attributes = new WebEventAttributes() ;
  
  public String getName() { return name; }
  public void setName(String name) { this.name = name; }
  
  public String getWebEventId() { return webEventId; }
  public void setWebEventId(String webEventId) { this.webEventId = webEventId; }
  
  public Date getTimestamp() { return timestamp; }
  public void setTimestamp(Date timestamp) { this.timestamp = timestamp; }
  
  public WebEventAttributes getAttributes() { return attributes; }
  public void setAttributes(WebEventAttributes attributes) { this.attributes = attributes; }

}
