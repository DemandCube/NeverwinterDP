package com.neverwinterdp.analytics.odyssey;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;

public class ActionEvent {
  private String source = "user";
  private String eventId;
  @JsonFormat(shape=JsonFormat.Shape.STRING, pattern="dd/MM/yyyy HH:mm:ss")
  private Date   timestamp;
  
  private String name;
  private String assignee;
  private String createdDate;
  private String dueDate;
  
  private String json;
  
  public String getSource() { return source; }
  public void setSource(String source) { this.source = source; }
  
  public String getEventId() { return eventId; }
  public void setEventId(String eventId) { this.eventId = eventId; }
  
  public Date getTimestamp() { return timestamp; }
  public void setTimestamp(Date timestamp) { this.timestamp = timestamp; }
  
  public String getName() { return name; }
  public void setName(String name) { this.name = name; }
  
  public String getAssignee() { return assignee; }
  public void setAssignee(String assignee) { this.assignee = assignee; }
  
  public String getCreatedDate() { return createdDate; }
  public void setCreatedDate(String createdDate) { this.createdDate = createdDate; }
  
  public String getDueDate() { return dueDate; }
  public void setDueDate(String dueDate) { this.dueDate = dueDate; }
  
  public String getJson() { return json; }
  public void   setJson(String json) { this.json = json; }
}