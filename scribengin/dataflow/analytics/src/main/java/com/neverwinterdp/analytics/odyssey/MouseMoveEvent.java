package com.neverwinterdp.analytics.odyssey;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;

public class MouseMoveEvent {
  private String source = "user";
  private String eventId;
  @JsonFormat(shape=JsonFormat.Shape.STRING, pattern="dd/MM/yyyy HH:mm:ss")
  private Date   timestamp;
  
  private String mouseMoveData ;
  
  public String getSource() { return source;}
  public void setSource(String source) { this.source = source; }
  
  public String getEventId() { return eventId; }
  public void setEventId(String eventId) { this.eventId = eventId; }
  
  public Date getTimestamp() { return timestamp; }
  public void setTimestamp(Date timestamp) { this.timestamp = timestamp; }
  
  public String getMouseMoveData() { return mouseMoveData; }
  public void setMouseMoveData(String mouseMoveData) { this.mouseMoveData = mouseMoveData; }
}
