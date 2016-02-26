package com.neverwinterdp.analytics.odyssey;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;

public class Event {
  private String eventId;
  
  @JsonFormat(shape=JsonFormat.Shape.STRING, pattern="dd/MM/yyyy HH:mm:ss")
  private Date   timestamp;
  private String json;
  
  public String getEventId() { return eventId; }
  public void setEventId(String eventId) { this.eventId = eventId; }
  
  public Date getTimestamp() { return timestamp; }
  public void setTimestamp(Date timestamp) { this.timestamp = timestamp; }
  
  public String getJson() { return json; }
  public void   setJson(String json) { this.json = json; }
}
